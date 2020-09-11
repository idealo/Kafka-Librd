#!/usr/bin/perl

use strict;
use warnings;
use Test::More;

use File::Temp qw(tempdir);
use Getopt::Long;
use IO::Socket;
use POSIX qw(WNOHANG);
use Scalar::Util qw(looks_like_number);
use Time::HiRes qw();

BEGIN {
    if (!eval q{ use Net::EmptyPort qw(empty_port); 1 }) {
	plan skip_all => 'Net::EmptyPort not available';
    }
    if (!eval q{ use IPC::Run qw(run); 1 }) {
	plan skip_all => 'IPC::Run not available';
    }
}

use Kafka::Librd qw();

# global conf
my $java = 'java';
my $kafka_dir = "/opt/kafka";
my $sample_topic = 'sampleTopic';
my $debug = undef;

GetOptions(
    "kafka-dir=s" => \$kafka_dir,
    "debug=s"     => \$debug,
)
    or die "usage: $0 [--kafka-dir /path/to/kafka] [--debug all|...]\n";

if (!-d $kafka_dir) {
    plan skip_all => "No kafka server directory available (expected in $kafka_dir), cannot test";
}

my $zookeeper_info = start_zookeeper();
my $kafka_info = start_kafka(zookeeper_info => $zookeeper_info);
require Data::Dumper; print STDERR "Line " . __LINE__ . ", File: " . __FILE__ . "\n" . Data::Dumper->new([$kafka_info],[qw()])->Indent(1)->Useqq(1)->Sortkeys(1)->Terse(1)->Dump; # XXX

system("$kafka_dir/bin/kafka-topics.sh", "--create", "--zookeeper", "localhost:$zookeeper_info->{port}", "--replication-factor", "1", "--partitions", "1", "--topic", "$sample_topic");

my $err;

my $producer = Kafka::Librd->new(
    Kafka::Librd::RD_KAFKA_PRODUCER,
    {
	'group.id' => 'producer_id',
	'queue.buffering.max.messages' => 1,
	($debug ? ('debug' => $debug) : ()),
    }
);
isa_ok $producer, 'Kafka::Librd';
$producer->brokers_add("localhost:$kafka_info->{port}");
note "Search for topic $sample_topic...";
my $topic = $producer->topic($sample_topic, {});
isa_ok $topic, 'Kafka::Librd::Topic';
my $sample_payload = "hello, world";
my $sample_key = "test-key-1";
$err = $topic->produce(-1, 0, $sample_payload, $sample_key);
diag "Produced message...";
$err and die "Couldn't produce: ", Kafka::Librd::Error::to_string($err);
is $err, 0;
diag "About to destroy producer...";
$producer->flush(10000);
$producer->destroy; # hmmm - maybe I have to force a flush?

my $consumer = Kafka::Librd->new(
    Kafka::Librd::RD_KAFKA_CONSUMER,
    {
	'group.id'                 => 'consumer_id',
	'enable.auto.offset.store' => 0,
	'enable.auto.commit'       => 0,
	'auto.offset.reset'        => 'earliest',
	($debug ? ('debug' => $debug) : ()),
    },
);
isa_ok $consumer, 'Kafka::Librd';
$consumer->brokers_add("localhost:$kafka_info->{port}");
$err = $consumer->subscribe([$sample_topic]);
$err and die "Couldn't subscribe to $sample_topic: ", Kafka::Librd::Error::to_string($err);
my $msg = $consumer->consumer_poll(10000);
if (!$msg) {
    die "Couldn't get message";
}
is $msg->err, 0;
is $msg->key, $sample_key;
is $msg->payload, $sample_payload;
is $msg->partition, 0;
is $msg->offset, 0;
is $msg->topic, $sample_topic;
ok looks_like_number $msg->timestamp;
{
    $msg->timestamp(\my $tstype);
    is $tstype, Kafka::Librd::RD_KAFKA_TIMESTAMP_CREATE_TIME();
}
eval {
    $msg->timestamp(\my @tstype);
};
like $@, qr{second argument tstype must be a scalar reference};

is $consumer->commit_message($msg), 0;
#diag $consumer->position($sample_topic);
my @tplist;
require Data::Dumper; diag(Data::Dumper->new([$consumer->committed(\@tplist,1000)],[qw()])->Indent(1)->Useqq(1)->Sortkeys(1)->Terse(1)->Dump); # XXX
require Data::Dumper; diag(Data::Dumper->new([\@tplist],[qw()])->Indent(1)->Useqq(1)->Sortkeys(1)->Terse(1)->Dump); # XXX
my $tplist = $consumer->subscription;
require Data::Dumper; diag(Data::Dumper->new([$tplist],[qw()])->Indent(1)->Useqq(1)->Sortkeys(1)->Terse(1)->Dump); # XXX
is $consumer->unsubscribe(), 0;

is $consumer->consumer_close, 0;

END {
    kill_kafka($kafka_info) if $kafka_info;
    kill_zookeeper($zookeeper_info) if $zookeeper_info;
}

sub start_zookeeper {
    my $zookeeper_port = empty_port();
    my $zookeeper_datadir = tempdir("zookeeper-data-XXXXXXXX", TMPDIR => 1, CLEANUP => 1);
    my $zookeeper_logdir = tempdir("zookeeper-log-XXXXXXXX", TMPDIR => 1, CLEANUP => 1);
    my $etc_zookeeper = tempdir("zookeeper-etc-XXXXXXXX", TMPDIR => 1, CLEANUP => 1);
    mkdir "$etc_zookeeper/conf";
    {
	open my $ofh, ">", "$etc_zookeeper/conf/zoo.cfg" or die $!;
	print $ofh <<EOF;
tickTime=2000
dataDir=$zookeeper_datadir
clientPort=$zookeeper_port
EOF
	close $ofh or die $!;
    }
    {
	open my $ofh, ">", "$etc_zookeeper/conf/log4j.properties" or die $!;
	print $ofh <<'EOF';
log4j.rootLogger=${zookeeper.root.logger}
log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
log4j.appender.CONSOLE.Threshold=INFO
log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n
log4j.appender.ROLLINGFILE=org.apache.log4j.RollingFileAppender
log4j.appender.ROLLINGFILE.Threshold=DEBUG
log4j.appender.ROLLINGFILE.File=${zookeeper.log.dir}/zookeeper.log
log4j.appender.ROLLINGFILE.MaxFileSize=10MB
log4j.appender.ROLLINGFILE.layout=org.apache.log4j.PatternLayout
log4j.appender.ROLLINGFILE.layout.ConversionPattern=%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n
log4j.appender.TRACEFILE=org.apache.log4j.FileAppender
log4j.appender.TRACEFILE.Threshold=TRACE
log4j.appender.TRACEFILE.File=${zookeeper.log.dir}/zookeeper_trace.log
log4j.appender.TRACEFILE.layout=org.apache.log4j.PatternLayout
log4j.appender.TRACEFILE.layout.ConversionPattern=%d{ISO8601} - %-5p [%t:%C{1}@%L][%x] - %m%n
EOF
	close $ofh or die $!;
    }

    # XXX from /usr/share/zookeeper/bin/zkEnv.sh
    my $classpath = "$etc_zookeeper/conf:/usr/share/java/jline.jar:/usr/share/java/log4j-1.2.jar:/usr/share/java/xercesImpl.jar:/usr/share/java/xmlParserAPIs.jar:/usr/share/java/netty.jar:/usr/share/java/slf4j-api.jar:/usr/share/java/slf4j-log4j12.jar:/usr/share/java/zookeeper.jar";
    my $zoomain = 'org.apache.zookeeper.server.quorum.QuorumPeerMain';
    my $zoocfg = "$etc_zookeeper/conf/zoo.cfg";

    # XXX from /usr/share/zookeeper/bin/zkServer.sh
    my @cmd = ($java,
	       "-Dzookeeper.log.dir=$zookeeper_logdir",
	       "-Dzookeeper.root.logger=INFO,ROLLINGFILE",
	       "-Djava.net.preferIPv4Stack=true",
	       "-cp", $classpath,
	       #$jvmflags,
	       $zoomain, $zoocfg);
    my $zookeeper_pid = fork;
    die $! if !defined $zookeeper_pid;
    if ($zookeeper_pid == 0) {
	diag "@cmd";
	exec @cmd;
	die $!;
    }

    diag "Waiting until zookeeper is running...\n";
    my $zookeeper_running;
    {
	my $MAX_TRIES = 100;
	for my $try (1..$MAX_TRIES) {
	    my $reaped_pid = waitpid($zookeeper_pid, WNOHANG);
	    if ($reaped_pid) {
		warn "Zookeeper process $zookeeper_pid prematurely exited";
		last;
	    }
	    my @check_cmd = ($java,
			     "-Dzookeeper.log.dir=$zookeeper_logdir", 
			     "-Dzookeeper.root.logger=INFO,ROLLINGFILE",
			     "-cp", $classpath,
			     # $jvmflags
			     "org.apache.zookeeper.client.FourLetterWordMain",
			     "localhost", $zookeeper_port, "srvr");
	    my $ret;
	    my $success = run(\@check_cmd, '2>', '/dev/null', '>', \$ret);
	    if ($success && $ret =~ /^Mode: standalone/m) {
		diag "Zookeeper is running:\n$ret";
		$zookeeper_running = 1;
	    }
	    last if $zookeeper_running;
	    if ($try < $MAX_TRIES) {
		Time::HiRes::sleep(0.1);
	    }
	}
    }
    die "zookeeper not running" if !$zookeeper_running;
    return {
	port => $zookeeper_port,
	pid  => $zookeeper_pid,
    };
}

sub kill_zookeeper {
    my $zookeeper_info = shift;
    my $pid = $zookeeper_info->{pid};
    diag "Stopping zookeeper pid $pid...\n";
    kill 9 => $pid;
}

sub start_kafka {
    my(%opts) = @_;
    my $zookeeper_info = delete $opts{zookeeper_info} || die "zookeeper_info is mandatory";
    die "Unhandled options: " . join(" ", %opts) if %opts;

    my $kafka_port = empty_port();
    my $kafka_logdir = tempdir("kafka-log-XXXXXXXX", TMPDIR => 1, CLEANUP => 1);
    my $kafka_configdir = tempdir("kafka-config-XXXXXXXX", TMPDIR => 1, CLEANUP => 1);
    open my $ofh, '>', "$kafka_configdir/server.properties" or die $!;
    print $ofh <<"EOF";
host.name=127.0.0.1
port=$kafka_port
zookeeper.connect=localhost:$zookeeper_info->{port}
broker.id=0
log.dir=$kafka_logdir
offsets.topic.replication.factor=1
EOF
    close $ofh or die $!;

    my $kafka_pid = fork;
    die if !defined $kafka_pid;
    if ($kafka_pid == 0) {
	my @cmd = (
	    "$kafka_dir/bin/kafka-server-start.sh",
	    "$kafka_configdir/server.properties",
	);
	diag "@cmd";
	exec @cmd;
	die $!;
    }

    diag "Waiting until kafka broker is running...\n";
    {
	my $MAX_TRIES = 100;
	for my $try (1..$MAX_TRIES) {
	    my $sock = IO::Socket->new(PeerHost => "localhost", PeerPort => $kafka_port, Domain => IO::Socket::AF_INET);
	    last if $sock;
	    if ($try < $MAX_TRIES) {
		Time::HiRes::sleep(0.1);
	    }
	}
    }

    return {
	port => $kafka_port,
	pid  => $kafka_pid,
    };
}

sub kill_kafka {
    my $kafka_info = shift;
    my $pid = $kafka_info->{pid};
    diag "Stopping kafka pid $pid...\n";
    kill 9 => $pid;
}

done_testing;

__END__
