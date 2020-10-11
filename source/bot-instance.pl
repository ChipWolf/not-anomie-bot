#!/usr/bin/perl -w

use strict;

# binmodes
$|=1;
binmode STDOUT, ':utf8';
binmode STDERR, ':utf8';

die "Cannot run as root\n" if $<==0;

my $scriptname=$0;
my @args=@ARGV;
my $botnum = shift or die "USAGE: $0 botnum task-dir\n";
my $dir = shift or die "USAGE: $0 botnum task-dir\n";
$dir.='/' unless substr($dir,-1) eq '/';

use Cwd;
use File::Basename;
use lib File::Basename::dirname( Cwd::realpath( __FILE__ ) );
use AnomieBOT::API;
use Sys::Hostname;
use POSIX ':sys_wait_h';
use sort 'stable';

my @tasks=();
my @tasknames=();
my %jobstatuses=();

chdir($AnomieBOT::API::basedir);

sub warnTS {
    warn POSIX::strftime('[%F %T] ', localtime), @_;
}

sub die2 {
    warnTS @_;
    exit(0);
}

my $api=AnomieBOT::API->new('conf.ini', $botnum);
$api->read_command();
$api->reopen_logs();

opendir(D, $dir) or die2 "($botnum) Could not open task directory: $!\n";
while(my $file=readdir(D)){
    next if -d $dir.$file;
    next if substr($file,0,1) eq '.';
    my $task='';
    if(!open(X, '<:utf8', $dir.$file)){
        warnTS "($botnum) Could not open task file $file: $!\n";
        next;
    }
    while(<X>){
        $task=$1 if /^package (.*);$/;
    }
    close(X);
    if($task eq ''){
        warnTS "($botnum) Invalid task file $file\n";
        next;
    }
    AnomieBOT::API::load($dir.$file);
    my $t=$task->new();
    next unless $t->approved == $botnum;
    push @tasks, $t;
    $task=~s/.*:://;
    push @tasknames, $task;
}
closedir(D);
unless(@tasks){
    warnTS "($botnum) No tasks\n";
    exit(0);
}

# Replace spaces with NBSP, gridengine doesn't handle spaces in $0 well.
($0=$api->user." ($botnum): ".join(' ', @tasknames))=~s/\s/\N{U+00A0}/g;
my $jobid = $ENV{'JOB_ID'} // 'n/a';
warnTS "Bot instance $botnum starting as job $jobid, pid $$ on " . hostname . "\n";

$SIG{QUIT}=sub { warnTS "($botnum) QUIT received!\n"; $api->halting('term'); };
$SIG{TERM}=sub { warnTS "($botnum) TERM received!\n"; $api->halting('term'); };
$SIG{INT}=sub { warnTS "($botnum) INT received!\n"; $api->halting('term'); };
$SIG{HUP}=sub { warnTS "($botnum) HUP received!\n"; $api->halting('restart'); };
$SIG{USR1}=sub { warnTS "($botnum) USR1 received! (ignoring)\n"; };
$SIG{USR2}=sub { warnTS "($botnum) (M) USR2 received!\n"; $api->halting('term'); };

# We need to somehow sort both @tasks and @tasknames. This does it by sorting
# indices first and then taking slices of both arrays.
my @i = sort { $tasks[$a]->{'order'} <=> $tasks[$b]->{'order'} } ( 0 .. $#tasks );
@tasks = @tasks[@i];
@tasknames = @tasknames[@i];
my @next=map { 0 } @tasks;

for my $taskname (@tasknames) {
    $jobstatuses{$taskname} = $api->cache->get( "status:$taskname" ) // { lastrun => 0, nextrun => 0 };
    $jobstatuses{$taskname}{'jobid'} = $jobid;
    $jobstatuses{$taskname}{'hostname'} = hostname;
    $jobstatuses{$taskname}{'botnum'} = $botnum;
    $jobstatuses{$taskname}{'status'} = 'job starting';
    $api->cache->set( "status:$taskname", $jobstatuses{$taskname} );
}

$api->onpause( sub {
    my $flag = shift;

    for(my $i=0; $i<@tasks; $i++){
        my $taskname = $tasknames[$i];
        next if $next[$i]<0;
        if ( $flag ) {
            next if $jobstatuses{$taskname}{'status'} eq 'paused';
            $jobstatuses{$taskname}{'pause saved status'} = $jobstatuses{$taskname}{'status'};
            $jobstatuses{$taskname}{'status'} = 'paused';
        } elsif ( exists( $jobstatuses{$taskname}{'pause saved status'} ) ) {
            $jobstatuses{$taskname}{'status'} = $jobstatuses{$taskname}{'pause saved status'};
            delete $jobstatuses{$taskname}{'pause saved status'};
        }
        $api->cache->set( "status:$taskname", $jobstatuses{$taskname} );
    }
} );

$api->login();
while(!$api->halting){
    while ( !$api->halting ) {
        my ($statuslist, $cas) = $api->cache->gets( 'joblist' );
        my $any = 0;
        for my $taskname (keys %jobstatuses) {
            $api->cache->add( "status:$taskname", $jobstatuses{$taskname} );
            unless ( grep $_ eq $taskname, @$statuslist ) {
                $any = 1;
                push @$statuslist, $taskname;
            }
        }
        last if !$any;
        warnTS "($botnum) Updating joblist\n";
        last if $api->cache->cas( 'joblist', $statuslist, $cas );
        sleep 1;
    }
    last if $api->halting;

    my $wait=60; # maximum wait time
    my $realwait=1e100;
    for(my $i=0; $i<@tasks; $i++){
        my $taskname = $tasknames[$i];
        next if($next[$i]<0 || $next[$i]>time());
        warnTS "($botnum) Starting task $taskname (".ref($tasks[$i]).")\n" if($api->DEBUG & 1);
        $jobstatuses{$taskname}{'status'} = 'running';
        $jobstatuses{$taskname}{'lastrun'} = time();
        $api->cache->set( "status:$taskname", $jobstatuses{$taskname} );
        my $w;
        my $terminated = 0;
        eval { $w=$tasks[$i]->run($api); };
        if($@){
            warnTS "($botnum) Caught error from task $taskname: $@\n";
            $jobstatuses{$taskname}{'status'} = 'error';
            $jobstatuses{$taskname}{'nextrun'} = 0;
            $terminated = 1;
        } elsif(!defined($w)){
            warnTS "($botnum) Task $taskname returned undef\n" if($api->DEBUG & 1);
            $jobstatuses{$taskname}{'status'} = 'ended';
            $jobstatuses{$taskname}{'nextrun'} = 0;
            $terminated = 1;
        } else {
            warnTS "($botnum) Task $taskname returned $w\n" if($api->DEBUG & 1);
            $next[$i]=time()+$w;
            $wait=$w if $w<$wait;
            $realwait=$w if $w<$realwait;
            $jobstatuses{$taskname}{'status'} = $tasks[$i]->status;
            $jobstatuses{$taskname}{'nextrun'} = $next[$i];
        }

        # Update job status
        $api->cache->set( "status:$taskname", $jobstatuses{$taskname} );
        if ( $terminated ) {
            delete $tasks[$i];
            # Indicate ended task in $0
            $tasknames[$i] = '(' . $tasknames[$i] . ')';
            ($0=$api->user." ($botnum): ".join(' ', @tasknames))=~s/\s/\N{U+00A0}/g;
            $next[$i]=-1;
        }

        last if $api->halting;
    }
    die2 "($botnum) No tasks" unless @tasks;
    warnTS "($botnum) Sleeping for $wait seconds\n" if($wait>0 && ($api->DEBUG & 1));
    last if $api->halting;
    $api->drop_connections() if $realwait>300;
    sleep($wait) if $wait>0;
}

for(my $i=0; $i<@tasks; $i++){
    my $taskname = $tasknames[$i];
    next if $next[$i]<0;
    $jobstatuses{$taskname}{'status'} = 'job ended';
    $jobstatuses{$taskname}{'nextrun'} = 0;
    $api->cache->set( "status:$taskname", $jobstatuses{$taskname} );
}

my $haltcode = $api->halting;

if($haltcode eq 'term'){
    warnTS "($botnum) Exiting now...\n";
    $api->DESTROY;
    warnTS "($botnum) Exited!\n";
    exit(0);
} elsif($haltcode =~ /^restart/){
    warnTS "($botnum) Restarting bot...\n";
    $api->DESTROY;
    warnTS "($botnum) Exited!\n";
    exec {$scriptname} ($scriptname,@args);
    warn "($botnum) Could not re-exec $scriptname: $!\n";
    exit(1);
} else {
    warnTS "($botnum) Exiting for unknown halt code $haltcode...\n";
    $api->DESTROY;
    warnTS "($botnum) Exited!\n";
    exit(2);
}
