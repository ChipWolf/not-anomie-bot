package AnomieBOT::API;

use utf8;
use strict;
use JSON;
use Time::HiRes qw/time sleep/;
use LWP::UserAgent;
use Net::OAuth;
use Bytes::Random::Secure ();
use HTTP::Message;
use Date::Parse;
use File::Spec;
use IPC::Run;
use Digest::MD5 qw/md5_hex/;
use POSIX;
use Carp;
use Encode qw/encode/;
use URI;
use Data::Dumper;
use DBI;
use Cwd qw/realpath/;
use AnomieBOT::API::Iterator;
use AnomieBOT::API::TiedDBD;
use AnomieBOT::API::Cache;

use vars qw/$default_maxlag/;
$default_maxlag=$ENV{'AnomieBOT_maxlag'} // 5;

# Old versions of URI don't handle unicode characters correctly. Detect that.
my $uri_need_encode;
my $uri=URI->new('http:');
$uri->query_form(x=>"\xa0");
if(lc($uri->query) eq 'x=%c2%a0'){
    $uri_need_encode=0;
} else {
    $uri=URI->new('http:');
    $uri->query_form(x=>encode('UTF-8',"\xa0"));
    if(lc($uri->query) eq 'x=%c2%a0'){
        $uri_need_encode=1;
    } else {
        die "URI is broken\n";
    }
}
undef($uri);

my $md5_need_encode;
if(lc(md5_hex("\xa0")) eq '8df6fbcc43d31d99e5112eb009ed8a2d'){
    $md5_need_encode=0;
} elsif(lc(md5_hex(encode('UTF-8',"\xa0"))) eq '8df6fbcc43d31d99e5112eb009ed8a2d'){
    $md5_need_encode=1;
} else {
    die "Digest::MD5 is broken\n";
}

=pod

=head1 NAME

AnomieBOT::API - AnomieBOT API access class

=head1 SYNOPSIS

 use AnomieBOT::API;

 my $api = AnomieBOT::API->new('/path/to/config_file', 1);
 $api->login();
 $res=$api->query(list=>'allpages',apnamespace=>0,aplimit=>10);

=head1 DESCRIPTION

C<AnomieBOT::API> is a class implementing various functions needed by a
MediaWiki bot.

=head1 RETURN VALUES

Unless otherwise noted, each method returns an object with certain standard
properties:

=over

=item code

A short token indicating the result of the API call. In addition to error codes
returnable by the MediaWiki API, the following may be seen:

=over

=item success

The call succeeded.

=item httperror

An HTTP error response was returned. The object will also contain a C<page>
property containing the full HTML returned with the error, and a C<httpcode>
property with the HTTP response code.

=item jsonerror

The response string could not be decoded. The object will also contain a C<raw>
property containing the response string;

=item wtferror

A "This can't happen" error occurred.

=item notloggedin

The bot user could not be logged in.

=item botexcluded

Returned by token-fetching functions if the page contains a bot exclusion
template that excludes this bot. The return object will have an extra property
C<type> with the value "bydefault", "byname", or "optout".

=item shutoff

Returned by token-fetching functions if the task's shutoff page is triggered.

=item pageprotected

Returned by token-fetching functions if the page is protected and the bot lacks
the appropriate rights.

=item pagemissing

Returned by token-fetching functions if the page does not exist (when a
non-existent page doesn't make sense).

=item uploadwarnings

Returned by the upload function if warnings were returned.

=item notiterable

Returned by C<< $api->iterator() >> when the result set cannot be iterated.

=item maxlag

If maxlag is set off or the bot is trying to halt, this error may be returned
instead of the bot automatically retrying.

=back

=item error

A textual description of the error code.

=back

=head1 METHODS

Note that this class supports runtime decoratoration, so the complete set of
available methods depends on the most recent call to C<< task() >>.

=over

=item AnomieBOT::API->new( $conffile, $botnum )

Create an instance of the bot.

The config file is simply a list of "key = value"s, one per line; any line
whose first non-whitespace character is '#' is considered a comment and is
ignored.

Sections within the file are delimited with C<< [I<name>] >>. Anything set
before the first section is considered "defaults" for any sections named "bot
I<#>".

In addition, a file may include other files by using a line
C<< @include I<opts> I<filename> >>. Any options begin with a "!" character;
the remainder of the line is the filename. By default, the file may have its
own defaults section (which is merged with the including file's) and sections;
if the "!section" option is given, the file will be considered part of the
current section and attempting to include other files or define sections will
be considered an error. If the "!ifexists" option is given, it will not be an
error if the file doesn't actually exist.

See A<conf.sample.ini> for parameters and defaults. The file permissions must
not include world readability, writability, or executability.

Botnum is the instance number of this bot, which controls which of the
C<[bot #]> sections of the config file is actually used.

=cut

sub _readconf {
    my ($file, $def, $CFG, $ssection) = @_;

    if ( open( my $fh, '<:utf8', $file ) ) {
        croak "Bad file permissions on $file" if (stat $fh)[2]&7;
        my $section = $ssection // '';
        while ( <$fh> ) {
            next if /^\s*#/;
            s/^\s+|\s+$//g;
            next if $_ eq '';
            if ( /^\@include\s+(.+)$/o ) {
                my $newfile = $1;
                if ( defined( $ssection ) ) {
                    croak $file.": Cannot include another file at line $.";
                } else {
                    my %opts = ( '!section' => 0, '!ifexists' => 0 );
                    $opts{$1} = 1 while $newfile=~s/^(!\S+)\s+//;
                    croak $file.": Invalid line at line $." if $newfile eq '';

                    my ($volume, $dir) = File::Spec->splitpath( $file );
                    my $base = File::Spec->catpath( $volume, $dir, '' );
                    $newfile = File::Spec->rel2abs( $newfile, $base );
                    next if $opts{'!ifexists'} && !-e $newfile;
                    _readconf( $newfile, $def, $CFG, $opts{'!section'} ? $section : undef );
                }
            } elsif ( /^\[([^\x5b\x5d]+)\]\s*$/o ) {
                if ( defined( $ssection ) ) {
                    croak $file.": Cannot begin a section at line $.";
                } else {
                    $section = $1;
                    $CFG->{$section} //= ($section=~/^bot \d+$/) ? { %$def } : {};
                }
            } elsif ( /^(\S+?)\s*=\s*(.*)$/o ) {
                if ( $section eq '' ) {
                    $def->{$1} = $2;
                } else {
                    $CFG->{$section}{$1} = $2;
                }
            } else {
                croak $file.": Invalid line at line $.";
            }
        }
        close $fh;
    } else {
        croak "Could not open ".$file.": $!";
    }
}

sub new {
    my $class = shift;
    my %CFG = ();

    croak "USAGE: AnomieBOT::API->new(\$file, \$botnum, [\\\%opt])" unless @_==2 or @_==3;

    my $botnum = $_[1];
    my $opt = $_[2] // {};
    my %def = ();
    _readconf( $_[0], \%def, \%CFG );

    carp "Bot instance number $botnum is not configured" unless exists($CFG{"bot $botnum"});

    my $umask=umask;

    # Check data directory
    my $datadir;
    if(exists($CFG{"bot $botnum"}{'datadir'})){
        $datadir=$CFG{"bot $botnum"}{'datadir'};
    } elsif(exists($ENV{'HOME'})){
        my $d=$ENV{'HOME'};
        $d.='/' unless substr($d,-1) eq '/';
        $d.='.anomiebot-data/';
        $datadir=$d;
    } else {
        die "HOME not set, please either set it or specify 'datadir' in the config file\n";
    }
    $datadir.='/' unless substr($datadir,-1) eq '/';
    if(!-e $datadir){
        umask($umask | 0007);
        my $ok = mkdir($datadir);
        umask($umask);
        die "Data directory ".$datadir." cannot be created: $!\n" unless $ok;
    }
    die "Data directory ".$datadir." is not a directory\n" unless -d $datadir;
    my $t=$datadir.'test'.$botnum;
    if(-e $t){
        unlink($t);
        die "Could not remove test file $t: $!\n" if -e $t;
    }
    open(X, '>', $t) or die("Could not create test file $t: $!\n");
    close(X);
    unlink($t);

    my $keep_alive = $CFG{"bot $botnum"}{'keep-alive'} // 1;
    $keep_alive = undef if $keep_alive < 0;
    my $cookiejar = $CFG{"bot $botnum"}{'cookiejar'} // '$DATADIR/AnomieBOT-$BOTNUM.cookies';
    $cookiejar=~s/\$BOTNUM/$botnum/g;
    $cookiejar=~s/\$DATADIR/$datadir/g;
    umask($umask | 0007);
    my $ua=LWP::UserAgent->new(
        agent=>"AnomieBOT/1.0 (no task; see [[User:".$CFG{"bot $botnum"}{'lguser'}."]])",
        from=>$CFG{"bot $botnum"}{'email'} // undef,
        cookie_jar=>{ file=>$cookiejar, autosave=>1 },
        env_proxy=>1,
        keep_alive=>$keep_alive,
    );
    $ua->cookie_jar->save();
    umask($umask);

    my $commandfile = $CFG{"bot $botnum"}{'commandfile'} // '$DATADIR/AnomieBOT-$BOTNUM.cmd';
    $commandfile=~s/\$BOTNUM/$botnum/g;
    $commandfile=~s/\$DATADIR/$datadir/g;
    my $logfile = $CFG{"bot $botnum"}{'logfile'} // '';
    $logfile=~s/\$BOTNUM/$botnum/g;
    $logfile=~s/\$DATADIR/$datadir/g;
    my $errfile = $CFG{"bot $botnum"}{'errfile'} // '';
    $errfile=~s/\$BOTNUM/$botnum/g;
    $errfile=~s/\$DATADIR/$datadir/g;

    die "Cannot use other users' home directory in commandfile\n" if $commandfile=~m!^~[^/]!;
    die "Cannot use other users' home directory in logfile\n" if $logfile=~m!^~[^/]!;
    die "Cannot use other users' home directory in errfile\n" if $errfile=~m!^~[^/]!;
    if(exists($ENV{'HOME'})){
        my $home = $ENV{'HOME'};
        $commandfile=~s!^~/!$home/!;
        $logfile=~s!^~/!$home/!;
        $errfile=~s!^~/!$home/!;
    } else {
        die "HOME is not set, cannot use ~/ in commandfile\n" if $commandfile=~m!^~/!;
        die "HOME is not set, cannot use ~/ in logfile\n" if $logfile=~m!^~/!;
        die "HOME is not set, cannot use ~/ in errfile\n" if $errfile=~m!^~/!;
    }

    my $memlimit = $CFG{"bot $botnum"}{'memlimit'} // '256M';
    my %suffix=(
        '' => 1,
        k => 1e3,
        m => 1e6,
        g => 1e9,
        K => 1024**1,
        M => 1024**2,
        G => 1024**3,
    );
    die "Invalid value for bot $botnum memlimit\n" unless $memlimit=~/^(\d+)([kmgKMG]?)$/;
    $memlimit = $1 * $suffix{$2};

    my $rand = Bytes::Random::Secure->new( NonBlocking => 1 );

    my $self = {
        botnum => $botnum,
        datadir => $datadir,
        ua => $ua,
        rand => $rand,
        j => JSON->new->utf8(0),
        task => 'no task',
        store => undef,
        wikibase => $CFG{"bot $botnum"}{'basepath'} // 'https://en.wikipedia.org/w/',
        lguser => $CFG{"bot $botnum"}{'lguser'} // '',
        lgpass => $CFG{"bot $botnum"}{'lgpass'} // '',
        oauth_consumer_token => $CFG{"bot $botnum"}{'oauth_consumer_token'} // '',
        oauth_consumer_secret => $CFG{"bot $botnum"}{'oauth_consumer_secret'} // '',
        oauth_access_token => $CFG{"bot $botnum"}{'oauth_access_token'} // '',
        oauth_access_secret => $CFG{"bot $botnum"}{'oauth_access_secret'} // '',
        email => $CFG{"bot $botnum"}{'email'} // undef,
        operator => $CFG{"bot $botnum"}{'operator'} // undef,
        read_throttle => 0,
        edit_throttle => 10,
        assert => $CFG{"bot $botnum"}{'assert'} // '',
        nassert => $CFG{"bot $botnum"}{'nassert'} // '',
        assert_edit => $CFG{"bot $botnum"}{'assert_edit'} // 'bot',
        nassert_edit => $CFG{"bot $botnum"}{'nassert_edit'} // '',
        use_encodings => !($CFG{"bot $botnum"}{'disable-transfer-encodings'} // 0),
        lastread => 0,
        lastedit => time(),
        debug => $CFG{"bot $botnum"}{'DEBUG'} // 0,
        automaxlag => 1,
        noedit => undef,
        nopause => 0,
        onpause => undef,
        editlimit => undef,
        decorators => [],
        queryprops => undef,
        queryprefix => undef,
        queryparams => undef,
        paramlimits => {},
        edit_watchlist => 'nochange',
        memlimit => $memlimit,
        commandfile => $commandfile,
        logfile => $logfile,
        errfile => $errfile,
        halting => 0,
        replica_dsn => $CFG{"bot $botnum"}{'replica_dsn'} // '',
        replica_user => $CFG{"bot $botnum"}{'replica_user'} // '',
        replica_pass => $CFG{"bot $botnum"}{'replica_pass'} // '',
    };

    $self->{'replica_dsn'}=~s/\$BOTNUM/$botnum/g;
    $self->{'replica_dsn'}=~s/\$DATADIR/$datadir/g;

    $self->{'use_oauth'} = $self->{'oauth_consumer_token'} ne '' &&
        $self->{'oauth_consumer_secret'} ne '' &&
        $self->{'oauth_access_token'} ne '' &&
        $self->{'oauth_access_secret'} ne '';

    # Create cache
    $self->{'cache'} = AnomieBOT::API::Cache->create(
        $CFG{"bot $botnum"}{'cache_handler'}//'Memcached',
        $CFG{"bot $botnum"}{'cache_options'}//'servers=127.0.0.1:112211;namespace=ChangeMe:',
    );

    # Open persistant storage
    $CFG{"bot $botnum"}{'store_dsn'}='dbi:SQLite:dbname=$DATADIR/AnomieBOT-$BOTNUM.db' unless exists($CFG{"bot $botnum"}{'store_dsn'});
    $CFG{"bot $botnum"}{'store_dsn'}=~s/\$BOTNUM/$botnum/;
    $CFG{"bot $botnum"}{'store_dsn'}=~s/\$DATADIR/$datadir/;
    $CFG{"bot $botnum"}{'store_user'}='' unless exists($CFG{"bot $botnum"}{'store_user'});
    $CFG{"bot $botnum"}{'store_pass'}='' unless exists($CFG{"bot $botnum"}{'store_pass'});
    if($opt->{'db'} // 1){
        umask($umask | 0007);
        $self->{'store'}=DBI->connect($CFG{"bot $botnum"}{'store_dsn'}, $CFG{"bot $botnum"}{'store_user'}, $CFG{"bot $botnum"}{'store_pass'}, { AutoCommit=>1, RaiseError=>1, mysql_auto_reconnect=>1 });
        umask($umask);
        die "Could not open database\n" unless $self->{'store'};
        $self->{'storetask'}=undef;
        $self->{'storehash'}=undef;
    }

    # Copy extra config sections for access by tasks
    $self->{'CFG'}={};
    while(my ($k,$v)=each %CFG){
        next if $k=~/^bot \d+$/;
        $self->{'CFG'}{$k}=$v;
    }

    bless $self, $class;
    return $self;
}

sub loadqueryprops {
    my $self=shift;
    return undef if defined($self->{'queryprops'});
    my $res=$self->_query(action=>'paraminfo',modules=>'query',__noassert=>1);
    return $res unless $res->{'code'} eq 'success';
    my @prop=();
    my @all=();
    for my $p (@{$res->{'paraminfo'}{'modules'}[0]{'parameters'}}) {
        @prop=@{$p->{'type'}} if $p->{'name'} eq 'prop';
        @all=(@all,@{$p->{'type'}}) if($p->{'name'} eq 'prop' || $p->{'name'} eq 'list' || $p->{'name'} eq 'generator' || $p->{'name'} eq 'meta');
    }
    @all=keys %{{ map { $_ => 1 } @all }};
    my %props=();
    my %prefix=();
    my %params=();
    while(@all){
        $res=$self->_query(action=>'paraminfo',modules=>join('|',map("query+$_",splice(@all,0,50))),__noassert=>1);
        return $res unless $res->{'code'} eq 'success';
        for my $p (@{$res->{'paraminfo'}{'modules'}}) {
            $props{$p->{'prefix'}}=$p->{'name'} if grep $_ eq $p->{'name'}, @prop;
            $prefix{$p->{'name'}}=$p->{'prefix'};
            $params{$p->{'name'}}={};
            for my $pp (@{$p->{'parameters'}}) {
                $params{$p->{'name'}}{$p->{'prefix'}.$pp->{'name'}} = $p->{'prefix'}.$pp->{'name'};
            }
        }
    }
    $self->{'queryprops'}=\%props;
    $self->{'queryprefix'}=\%prefix;
    $self->{'queryparams'}=\%params;
    return undef;
}

=pod

=item $api->copy( %config )

Get a new AnomieBOT::API with the specified options changed.

=cut

sub copy {
    my ($self, %config) = @_;
    my $new = {
        %$self,
        storetask => undef,
        storehash => undef,
        %config
    };
    bless $new, ref $self;
    return $new;
}

=pod

=item $api->DEBUG

=item $api->DEBUG( $bitmask )

Get/set the debug bitmask. When debugging is enabled, most methods will output
useful text to standard error.

Returns the old value of the bitmask.

=cut

sub DEBUG {
    my $self=shift;
    my $old=$self->{'debug'};
    if(@_){
        croak "Invalid DEBUG bitmask: $_[0]" unless $_[0]=~/^[+-]?\d+$/;
        $self->{'debug'}=$_[0];
    }
    return $old;
}

=pod

=item $api->reopen_logs()

Reopen STDOUT and STDERR to the configured log files.

=cut

sub reopen_logs {
    my $self=shift;

    if($self->{'logfile'} ne ''){
        open(STDOUT, '>>', $self->{'logfile'}) or die "Cannot redirect STDOUT: $!\n";
    }
    if($self->{'errfile'} ne ''){
        my $olderr;
        open($olderr, ">&STDERR") or die "Cannot dup STDERR: $!\n";
        if(!open(STDERR, '>>', $self->{'errfile'})) {
            # Output the error message to the old STDERR
            print $olderr "Cannot redirect STDERR: $!\n";
            exit 1;
        }
        close($olderr);
    }

    my $fh = select;
    binmode STDOUT, ':utf8';
    select STDOUT; $| = 1;
    binmode STDERR, ':utf8';
    select STDERR; $| = 1;
    select $fh;
}

=pod

=item $api->warn( $message )

=item AnomieBOT::API->warn( $message )

=item $api->log( $message )

=item AnomieBOT::API->log( $message )

=item $api->debug( $bitmask, $message )

=item AnomieBOT::API->debug( $debuglevel, $bitmask, $message )

Output messages.

=cut

sub warn {
    my $self=shift;
    $self={ task=>'static', botnum=>0 } unless ref($self);
    my $msg=shift;
    if(-t STDERR){
        my $nl='';
        $nl="\n" if $msg=~s/\n$//;
        carp "\e[31m".POSIX::strftime('[%F %T] ', localtime).$self->{'task'}.' ('.$self->{'botnum'}."): $msg\e[0m$nl";
    } else {
        carp POSIX::strftime('W [%F %T] ', localtime).$self->{'task'}.' ('.$self->{'botnum'}.'): '.$msg;
    }
}

sub log {
    my $self=shift;
    $self={ task=>'static', botnum=>0 } unless ref($self);
    my $msg=shift;
    print POSIX::strftime('[%F %T] ', localtime).$self->{'task'}.' ('.$self->{'botnum'}.'): '.$msg."\n";
}

sub debug {
    my $self=shift;
    $self={ task=>'static', botnum=>0, debug=>shift } unless ref($self);
    my $mask=shift;
    my $msg=shift;
    if($self->{'debug'} & $mask){
        if(-t STDERR){
            my $nl='';
            $nl="\n" if $msg=~s/\n$//;
            carp "\e[33m".POSIX::strftime('[%F %T] ', localtime).$self->{'task'}.' ('.$self->{'botnum'}."): $msg\e[0m$nl";
        } else {
            carp POSIX::strftime('D [%F %T] ', localtime).$self->{'task'}.' ('.$self->{'botnum'}.'): '.$msg;
        }
    }
}

=pod

=item $api->check_commands()

=item $api->check_commands( $file )

Check if there are any pending commands for this instance. Returns the command,
if any, unless the command was "KILL".

An optional filename may be passed to override the normal command file.

=item $api->read_command()

=item $api->read_command( $file )

=item $api->read_command( $file, $nodelete )

Read any pending command; to be called on startup. Returns the command, if
any. The pending command is cleared, unless it matches the C<$nodelete>
regexp.

An optional filename may be passed to override the normal command file.

=item $api->send_command( $file, $command )

Sends a command to the specified file, overwriting any pending command (if
any). Returns true on success, undef on failure.

=item $api->onpause()

=item $api->onpause( \&sub )

Get/set a subroutine to be called when the "pause" command is received. The
subroutine will be passed a single argument, 1 when the pause command is
received and 0 when it is cleared.

Returns the old subroutine, if any.

=cut

sub onpause {
    my $self = shift;
    my $ret = $self->{'onpause'};
    if ( @_ ) {
        $self->{'onpause'} = shift;
    }
    return $ret;
}

sub check_commands {
    my $self = shift;
    my $file = shift // $self->{'commandfile'};
    my $cmd = $self->read_command( $file, qr/^pause$|^restart-hash/ );

    return undef unless defined( $cmd );

    local $self->{'task'} = '[command processor]';
    if($cmd =~ '^restart-hash ([0-9a-f]{40})$'){
        my $hash = $1;
        my $curhash = '';
        my $t = IPC::Run::timeout( 5 );
        $t->exception( 'git rev-parse timed out' );
        eval {
            IPC::Run::run( [qw(git rev-parse HEAD)], '>', \$curhash, $t ) or die "Git rev-parse failed with code $?\n";
        };
        if ( $@ ) {
            $self->warn( $@ );
            return undef;
        }
        chomp $curhash;
        if ( $curhash eq $hash ) {
            $self->warn("Restarting to update to git hash $hash!");
            $self->{'halting'}=$cmd;
            $self->read_command( $file, qr/^(?!\Q$cmd\E\s*$)/ );
            return $cmd;
        }
        return undef;
    } elsif($cmd eq 'restart'){
        $self->warn("Restart signal received!");
        $self->{'halting'}=$cmd;
    } elsif($cmd eq 'term'){
        $self->warn("Halt signal received!");
        $self->{'halting'}=$cmd;
    } elsif($cmd eq 'kill'){
        $self->warn("Kill signal received!");
        exit(0);
    } elsif($cmd eq 'pause'){
        if($self->{'nopause'}){
            #$self->warn("Ignoring pause signal");
            #$self->send_command( $file, undef );
        } else {
            $self->warn("Pause signal received! Pausing until $file is changed.");
            $self->{'onpause'}(1) if $self->{'onpause'};
            while($cmd eq 'pause'){
                sleep(1);
                $cmd = $self->read_command( $file, qr/^/ ) // '';
            }
            $self->warn("Pause signal cleared!");
            $self->{'onpause'}(0) if $self->{'onpause'};
            return $self->check_commands( $file );
        }
    } elsif($cmd eq 'reopen'){
        $self->log("Reopening log files");
        $self->warn("Reopening log files");
        $self->reopen_logs();
        $self->log("Log files reopened");
        $self->warn("Log files reopened");
    } elsif($cmd eq 'ping'){
        $self->log("Pong!");
        $self->warn("Pong!");
    } elsif($cmd =~ /^debug\s+([+-]?\d+)$/){
        my $old = $self->DEBUG($1);
        $self->warn("Set DEBUG = $1, was $old\n");
    } elsif($cmd =~ /^debug(?:\s.*)?$/){
        $self->warn("Invalid 'debug' command");
    } else {
        $self->warn("Received unknown command '$cmd'\n");
    }

    return $cmd;
}

sub read_command {
    my $self = shift;
    my $file = shift // $self->{'commandfile'};
    my $nodelete = shift // qr/^(?!)/;
    my $cmd = undef;

    if ( $file =~ /^cache:(.*)/ ) {
        my $key = $1;
        $cmd = $self->cache->get( $key );
        $self->cache->delete( $key ) if defined( $cmd ) && $cmd !~ /$nodelete/;
    } else {
        my $fh;
        if(open($fh, "<", $file)) {
            $cmd = <$fh>;
            close($fh);
            $cmd=~s/^\s+|\s+$//g;
            unlink( $file ) if $cmd !~ /$nodelete/;
        }
    }

    return $cmd;
}

sub send_command {
    my $self = shift;
    my $file = shift;
    my $cmd = shift;

    if ( $file =~ /^cache:(.*)/ ) {
        if ( defined( $cmd ) ) {
            $self->cache->set( $1, $cmd );
        } else {
            $self->cache->delete( $1 );
        }
    } else {
        if ( defined( $cmd ) ) {
            my $fh;
            return undef unless open($fh, ">", $file);
            print $fh $cmd;
            close($fh);
        } else {
            unlink( $file );
        }
    }

    return 1;
}

=pod

=item $api->halting

Process commands, then return a true value if the bot is supposed to halt.

=item $api->halting( $value )

Set the return value for future calls to halting().

=cut

sub halting {
    my $self = shift;
    if(@_){
        $self->{'halting'} = shift;
    } else {
        $self->check_commands();
    }
    return $self->{'halting'};
}

=pod

=item $api->automaxlag

=item $api->automaxlag( $bool )

Get/set the auto-continue maxlag flag. Note that task() resets this.

Returns the old value of the flag.

=cut

sub automaxlag {
    my $self=shift;
    my $old=$self->{'automaxlag'};
    $self->{'automaxlag'}=$_[0]?1:0 if(@_);
    return ($old && !$self->halting);
}

=pod

=item $api->edit_watchlist

=item $api->edit_watchlist( $value )

Get/set the editing C<watchlist> value. Valid values are specified by the API,
currently C<watch>, C<unwatch>, C<preferences>, or C<nochange>. The default is
C<nochange>.

Returns the old value.

=cut

sub edit_watchlist {
    my $self=shift;
    my $old=$self->{'edit_watchlist'};
    $self->{'edit_watchlist'}=$_[0] if(@_);
    return $old;
}

=pod

=item $api->user

Returns the bot user name.

=item $api->operator

Returns the bot operator's user name.

=cut

sub user {
    my $self=shift;
    return $self->{'lguser'};
}

sub operator {
    my $self=shift;
    return $self->{'operator'};
}

=pod

=item $api->task

=item $api->task( $name )

=item $api->task( $name, $read_rate, $write_rate, @decoraters )

Get/set the current task.

When called with no arguments, simply returns the current task name.

With arguments, it sets the task name, read/write rate limits, and the current
list of decorators. If omitted, it defaults to 0 seconds per read, 10 seconds
per write, and no decorators.

=item $api->read_throttle

=item $api->read_throttle( $seconds )

Get/set the current read throttle time. If a read is attempted less than
$seconds seconds after a previous read or edit, the bot will sleep for the
remaining time.

Returns the old throttle.

=item $api->edit_throttle

=item $api->edit_throttle( $seconds )

Get/set the current edit throttle time. If an edit is attempted less than
$seconds seconds after a previous read or edit, the bot will sleep for the
remaining time.

Returns the old throttle.

=item $api->decorators

=item $api->decorators( @decorators )

Get/set the current list of decorators. Note that functions are first searched
for in the current object (thus a decorator cannot override native functions),
then in each decorator I<in order>. If you want to set an empty list of
decorators, pass undef as the only argument.

Returns the old list of decorators.

=cut

sub task {
    my $self=shift;
    my $old=$self->{'task'};
    if(@_){
        $self->{'task'}=shift;
        $self->{'ua'}->agent("AnomieBOT/1.0 (".encode('UTF-8',$self->{'task'})."; see [[User:".$self->{'lguser'}."]])");
        $self->debug(1, 'Beginning task');
        $self->read_throttle(shift // 0);
        $self->edit_throttle(shift // 10);
        $self->decorators(@_ ? @_ : undef);
        $self->automaxlag(1);
    }
    return $old;
}

sub read_throttle {
    my $self=shift;
    my $old=$self->{'read_throttle'};
    if(@_){
        my $n=shift;
        if($n!~/^(?:\d*\.)?\d+$/ || $n<0){
            carp "Time value for read_throttle must be a non-negative floating point number.";
        } else {
            $self->{'read_throttle'}=0.0+$n;
            $self->debug(1, "Read throttle set to $n seconds");
        }
    }
    return $old;
}

sub edit_throttle {
    my $self=shift;
    my $old=$self->{'edit_throttle'};
    if(@_){
        my $n=shift;
        if($n!~/^(?:\d*\.)?\d+$/ || $n<0){
            carp "Time value for edit_throttle must be a non-negative floating point number.";
        } else {
            $self->{'edit_throttle'}=0.0+$n;
            $self->debug(1, "Edit throttle set to $n seconds");
        }
    }
    return $old;
}

sub decorators {
    my $self=shift;
    my $old=$self->{'decorators'};
    if(@_){
        $self->{'decorators'}=[defined($_[0])?@_:()];
        $self->debug(1, "Decorators: ".join(', ', @{$self->{'decorators'}}));
        load($_) foreach (@{$self->{'decorators'}});
    }
    return @$old;
}

sub _throttle {
    my $self=shift;
    my $which=shift;
    my $t;

    if($which eq 'read'){
        $t=$self->{'read_throttle'}-(time()-$self->{'lastread'});
        sleep($t) if $t>0;
        $self->{'lastread'}=time();
    } elsif($which eq 'edit'){
        $t=$self->{'edit_throttle'}-(time()-$self->{'lastedit'});
        sleep($t) if $t>0;
        $self->{'lastedit'}=time();
        $self->{'lastread'}=$self->{'lastedit'};
    }
}

=pod

=item $api->is_trial

Returns a true value if a trial is running. This can be used to disable an
unapproved code addition in the live code while running it for trial.

=cut

sub is_trial {
    my $self=shift;
    return defined($self->{'editlimit'});
}

=pod

=item $api->CFG

Access the configuration settings for the current task. The most common use
will be along the lines of C<< $api->CFG->{$property} >>.

=cut

sub CFG {
    my $self=shift;
    $self->{'CFG'}{$self->{'task'}}={} unless exists($self->{'CFG'}{$self->{'task'}});
    return $self->{'CFG'}{$self->{'task'}};
}

=pod

=item $api->store

Returns a hashref tied to persistant storage corresponding to the current task,
or undef if no task is set.

Since this is tied to persistant storage, only scalars (no scalar refs),
hashrefs, and arrayrefs may be stored in the array. Anything else will cause a
fatal error.

=cut

sub store {
    my ($self) = @_;
    return undef unless defined($self->{'task'});
    if(!defined($self->{'storetask'}) || $self->{'task'} ne $self->{'storetask'}){
        untie %{$self->{'storehash'}};
        $self->{'storetask'}=$self->{'task'};
        tie %{$self->{'storehash'}}, 'AnomieBOT::API::TiedDBD', $self->{'store'}, 'AnomieBOT_Store', 'k', 'v', task => $self->{'storetask'};
    }
    return $self->{'storehash'};
}

=pod

=item $api->cache

Returns an A<AnomieBOT::API::Cache> object, for non-persistant data storage.

=cut

sub cache {
    my ($self) = @_;
    return $self->{'cache'};
}

=pod

=item $api->connectToReplica( $wiki )

=item $api->connectToReplica( $wiki, $svc )

Calls C<< DBI->connect >> and returns the database handle.

If C<replica_dsn> is not set in conf.ini, dies instead.

C<$svc> replaces a C<$SVC> token in C<replica_dsn>. For Tool Forge, pass 'analytics' or 'web'. Defaults to 'web'.

=cut

sub connectToReplica {
    my $self = shift;
    my $wiki = shift;
    my $svc = shift || 'web';

    my $dsn = $self->{'replica_dsn'};
    carp "'replica_dsn' is not set\n" if $dsn eq '';
    $dsn=~s/\$WIKI/$wiki/g;
    $dsn=~s/\$SVC/$svc/g;
    my $dbh = DBI->connect($dsn, $self->{'replica_user'}, $self->{'replica_pass'}, { AutoCommit=>1, RaiseError=>1, mysql_auto_reconnect=>1 });

    return wantarray ? ($dbh) : $dbh;
}

=pod

=item $api->drop_connections

Drops any connections in the Keep-Alive state. Call this if you will not be
making API calls for the next 300 seconds.

=cut

sub drop_connections {
    my $self=shift;
    $self->{'ua'}->conn_cache->drop();
}

=pod

=item $api->rawpage( $title )

=item $api->rawpage( $title, $oldid )

Get the raw wikitext of a page, specified by title and (optionally) revision
id. The return object has the following additional properties:

=over

=item content

Content of the page

=back

As with C<query()>, this method may pause for read throttling or maxlag errors.

=cut

sub rawpage {
    my @args=@_;
    my $self = shift @args;
    my $title = shift @args;

    my %q = @args ? ( 'revids' => $args[0] ) : ( 'titles' => $title, 'rvlimit' => 1 );

    my $res = $self->query(
        prop => 'revisions',
        rvprop => 'content',
        rvslots => 'main',
        %q,
    );
    if ( $res->{'code'} eq 'success' ) {
        return {
            code    => 'success',
            error   => 'Success',
            content => (values %{$res->{'query'}{'pages'}})[0]{'revisions'}[0]{'slots'}{'main'}{'*'} // '',
        };
    } else {
        return $res;
    }
}

sub _ISO2wptime {
    my $t=shift;
    return $1.$2.$3.$4.$5.$6
        if $t=~/^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)Z$/;
}

sub _query {
    my ($self, %param) = @_;

    my $nolog=(exists($param{'__nolog'}) && $param{'__nolog'});
    my $errok=(exists($param{'__errok'}) && $param{'__errok'});
    my $noassert=(exists($param{'__noassert'}) && $param{'__noassert'});
    delete $param{'__nolog'};
    delete $param{'__errok'};
    delete $param{'__noassert'};

    my $raw=exists($param{'format'});
    $param{'format'}='json' unless $raw;
    $param{'maxlag'}=$default_maxlag unless exists($param{'maxlag'});
    $param{'rawcontinue'}=1 if($param{'action'} eq 'query' && !exists($param{'continue'}));

    $param{'assert'}=$self->{'assert'} if(!$noassert && !exists($param{'assert'}) && $self->{'assert'} ne '');
    $param{'nassert'}=$self->{'nassert'} if(!$noassert && !exists($param{'nassert'}) && $self->{'nassert'} ne '');

    my %h=(
        'Content_Type' => 'form-data',
        #'X-Wikimedia-Debug' => 'backend=mwdebug1001.eqiad.wmnet; log',
    );
    while(my ($k,$v)=each %param){
        $param{$k}=encode('UTF-8', $v) if($uri_need_encode && !ref($v));
    }
    if($self->{'use_encodings'}) {
        my $can_accept=HTTP::Message::decodable;
        $h{'Accept-Encoding'}=$can_accept if $can_accept;
    }

    my $url = $self->{'wikibase'}.'api.php';
    if ( $self->{'use_oauth'} ) {
        my $nonce = $self->{'rand'}->bytes_base64( 15 );
        $nonce =~ y!+/\n!-_!;
        # Note: POST data isn't included here because AnomieBOT always uses
        # multipart/form-data
        my $request = Net::OAuth->request( 'protected resource' )->new(
            request_method => 'POST',
            request_url => $url,
            consumer_key => $self->{'oauth_consumer_token'},
            consumer_secret => $self->{'oauth_consumer_secret'},
            token => $self->{'oauth_access_token'},
            token_secret => $self->{'oauth_access_secret'},
            signature_method => 'HMAC-SHA1',
            timestamp => int(time()),
            nonce => $nonce,
        );
        $request->sign;
        $h{'Authorization'} = $request->to_authorization_header;
    }

    my $res=$self->{'ua'}->post($url, \%param, %h);

    my $q;
    if($nolog){
        $q='[unlogged]';
    } else {
        $q=Dumper(\%param);
        $q=~s/\n\s*/ /g;
    }
    if($res->code!=200){
        $self->warn("Query failed: ".$res->status_line);
        #$self->warn("Failed query was $q");
        return {
            code     => 'httperror',
            httpcode => $res->code,
            error    => $res->status_line,
            page     => $res->decoded_content
        };
    }
    if($raw){
        $self->debug(1, "Query $q");
        return {
            code    => 'success',
            error   => 'Success',
            content => $res->decoded_content
        };
    }

    my $ret;
    eval { $ret=$self->{'j'}->utf8->decode($res->decoded_content // ''); };
    if($@){
        $self->warn("JSON decoding failed: $@");
        #$self->warn("Failed query was $q");
        return {
            code  => 'jsonerror',
            error => $@,
            page  => $res->decoded_content
        };
    }
    $ret={ '*' => $ret } if ref($ret) ne 'HASH';
    if($ret->{'error'}){
        if($ret->{'error'}{'code'} eq 'maxlag'){
            $ret=$self->_handle_maxlag($res, $ret->{'error'}{'info'});
            return $ret if defined($ret);
            goto \&_query;
        } elsif($ret->{'error'}{'code'} =~ /^assert.*failed/){
            # Assertion failed. Maybe we're not logged in?
            my $ret2=$self->_query(action=>'query', meta=>'userinfo', maxlag => 600, __noassert => 1);
            return {
                code  => $ret->{'error'}{'code'},
                error => $ret->{'error'}{'info'}
            } if($ret2->{'code'} eq 'success' && $ret2->{'query'}{'userinfo'}{'name'} eq $self->{'lguser'});
            $ret2 = $self->login(1);
            return $ret2 unless $ret2->{'code'} eq 'success';
            goto \&_query;
        } else {
            unless(ref($errok) eq 'ARRAY' && grep $_ eq $ret->{'error'}{'code'}, @$errok){
                $self->debug(1, "Query $q");
                $self->warn("MediaWiki error: ".$ret->{'error'}{'info'});
                #$self->warn("Failed query was $q");
            }
            my %ret2 = %{$ret->{'error'}};
            $ret2{'servedby'} = $ret->{'servedby'}//'unknown';
            $ret2{'error'} = $ret->{'error'}{'info'};
            delete($ret2{'info'});
            return \%ret2;
        }
    }
    if($ret->{'warnings'}){
        $self->debug(1, "Query $q");
        while ( my ($module, $warning) = each (%{$ret->{'warnings'}}) ) {
            $self->warn("MediaWiki warning: $module: " . $warning->{'*'});
        }
    }
    $self->debug(1, "Query $q");
    $ret->{'code'}='success';
    $ret->{'error'}='Success';
    return $ret;
}

=pod

=item $api->query( key => value, ... )

=item $api->query( \@continues, key => value, ... )

Perform a general MediaWiki API query.

As you should be aware, if an L<action=query|mw:API:Query> API call is going to
return an excessive amount of data, only the first bit will be returned and
various continuation parameters will be returned to get the next bit in a
future call. The array reference C<\@continues> specifies how to handle that:
those multiple calls will be made automatically for all modules I<not> named.
Note that you will get errors if your query uses a generator and you specify
any non-generator modules without also specifying the generator module. To
specify the generator module, prefix the module name with "g|". If for some
reason you need to specify the continuation parameter explicitly, join it to
the module name with a "|" (e.g. "revisions|rvcontinue"); note this is not
recommended.

If C<\@continues> is not provided, the default value consists of the
generator or list modules specified in the query, plus "revisions|rvcontinue"
(we don't want to download all several-thousand revisions automatically when
prop=revisions is in enum mode).

The keys and values are those needed for the API call, with the following
exceptions:

=over

=item action

If omitted, "query" will be assumed. Do not use "login", "edit", "move", or
others that have a method provided.

=item format

If not specified, the json-format response will be decoded as a Perl object,
the standard properties will be added, and the object will be returned. This is
most likely what you want.

If specified (even as "json"), the raw response text will be returned in the
C<content> property of the return object. MediaWiki errors will not be
detected.

=item maxlag

If unspecified, the default value "5" will be used. Maxlag errors are (usually)
automatically retried.

=item __nolog

If specified and true, the query will not be output to the bot log (when
debugging is active). Use this if the query contains passwords or other
sensitive information.

=item __errok

Specify an array of MediaWiki error codes that will not be logged. Use this if
you expect the query might fail and don't want to clutter the log. For example,
you might set C<['editconflict']> when calling L<action=edit|mw:API:Edit> if
you were expecting edit conflicts.

=back

The return value is normally the API response object, but see above for
details.

=cut

sub query {
    my $self = shift;
    my (@continues, %param);
    if(ref($_[0]) eq 'ARRAY'){
        @continues = @{shift()};
        %param = @_;
    } else {
        %param = @_;
        @continues = ('revisions|rvcontinue');
        push @continues, $param{'list'} if exists($param{'list'});
        push @continues, 'g|'.$param{'generator'} if exists($param{'generator'});
    }

    $param{'action'}='query' unless exists($param{'action'});
    if(grep $param{'action'} eq $_, qw/edit move login logout upload/){
        my $e="Use AnomieBOT::API->".$param{'action'}." instead of passing action=".$param{'action'}." to AnomieBOT::API->query";
        carp $e;
        return {
            code  => 'params',
            error => $e
        };
    }

    # Split the query prop/list/meta params into several sets
    my $qpres=$self->loadqueryprops;
    return $qpres if defined($qpres);

    my %propparams = ();
    my %listparams = ();
    my %map = ();
    if ($param{'action'} eq 'query') {
        %map = (
            prop => \%propparams,
            list => \%listparams,
            meta => \%listparams,
        );
        while (my ($prop, $list) = each %map) {
            if (($param{$prop}//'') ne '') {
                $list->{$prop} = $param{$prop};
                delete($param{$prop});
                for my $p (split /\|/, $list->{$prop}) {
                    my $pp = $self->{'queryparams'}{$p};
                    while (my ($k,$v) = each(%param)) {
                        next unless exists($pp->{$k});
                        $list->{$k} = $v;
                        delete($param{$k});
                    }
                }
            }
        }
    }

    # Is a query, so loop over specified continuations. To do it right, we need
    # nested loops: the inner loop runs over all non-generator continuations to
    # completion, and the outer loop runs over the generator continuation.
    my $ret={};
    my %c2=();
    my %retc=();
    my %savepropparams = %propparams;
    do {
        # First, do non-generators
        my %gc=();
        my %c=();
        do {
            # Perform query: passed params, the "prop=??" value, and continues
            $self->_throttle('read');
            my $res=$self->_query(%param, %propparams, %listparams, %c, %c2);
            return $res if($res->{'code'} ne 'success');

            # Process query-continue
            %c=();
            my %p=();
            if(exists($res->{'query-continue'})){
                my $qpres=$self->loadqueryprops;
                return $qpres if defined($qpres);
                my %prefixes=%{$self->{'queryprefix'}};
                while(my ($p,$n)=each(%{$res->{'query-continue'}})){
                    my $prefix=$prefixes{$p}//'**';
                    while(my ($k,$v)=each(%$n)){
                        my $g=0;
                        if(substr($k,0,length($prefix)) eq $prefix){
                            # Normal parameter
                            $g=grep(($p eq $_ || "$p|$k" eq $_), @continues);
                        } elsif(substr($k,0,length($prefix)+1) eq "g$prefix"){
                            # Generator parameter; at this point, don't continue it.
                            $g=1;
                        } else {
                            # WTF?
                            $self->warn("Got continuation parameter $k for module $p");
                        }
                        if($g){
                            # Return this one to the caller
                            $gc{$p}={} if(!exists($gc{$p}));
                            $gc{$p}{$k}=$v if(!exists($gc{$p}{$k}));
                            if($gc{$p}{$k} ne $v){
                                my $e="$p.$k of $v doesn't match previous call's value of ".$gc{$p}{$k}."!";
                                CORE::warn("\e[31;01m$e\e[0m");
                                return {
                                    code  => 'wtferror',
                                    error => $e,
                                };
                            }
                        } else {
                            # Continue on this one
                            $c{$k}=$v;
                            $p{$p}=1;
                        }
                    }
                }
                delete $res->{'query-continue'};
            }

            # Update prop/meta/list", only keep the ones we're continuing on
            while (my ($prop, $list) = each %map) {
                next if ($list->{$prop}//'') eq '';
                my @keep = ();
                for my $pn (split /\|/, $list->{$prop}) {
                    if (exists($p{$pn})) {
                        push @keep, $pn;
                    } else {
                        for my $k (keys %{$self->{'queryparams'}{$pn}}) {
                            delete($list->{$k});
                        }
                    }
                }
                if ( @keep ) {
                    $list->{$prop} = join('|', @keep);
                } else {
                    delete($list->{$prop});
                }
            }

            # Merge queries
            _mergequeries($ret, $res);
        } while(%c);

        # Now, continue any generators that need continuing
        %c2=();
        if(%gc){
            my %prefixes=%{$self->{'queryprefix'}};
            while(my ($p,$n)=each(%gc)){
                my $prefix=$prefixes{$p}//'**';
                while(my ($k,$v)=each(%$n)){
                    my $g=0;
                    if(substr($k,0,length($prefix)) eq $prefix){
                        # Normal parameter
                        $g=grep(($p eq $_ || "$p|$k" eq $_), @continues);
                    } elsif(substr($k,0,length($prefix)+1) eq "g$prefix"){
                        # Generator parameter
                        $g=grep(("g|$p" eq $_ || "g|$p|$k" eq $_), @continues);
                    } else {
                        # WTF?
                        $self->warn("Got continuation parameter $k for module $p");
                    }
                    if($g){
                        # Return this one to the caller
                        $retc{$p}={} if(!exists($retc{$p}));
                        $retc{$p}{$k}=$v if(!exists($retc{$p}{$k}));
                        if($retc{$p}{$k} ne $v){
                            my $e="$p.$k of $v doesn't match previous call's value of ".$retc{$p}{$k}."!";
                            CORE::warn("\e[31;01m$e\e[0m");
                            return {
                                code  => 'wtferror',
                                error => $e,
                            };
                        }
                    } else {
                        # Continue on this one
                        $c2{$k}=$v;
                    }
                }
            }
            # Reset the prop parameters for the generator continuation
            %propparams = %savepropparams;
        }
    } while(%c2);

    # Add the returnable continue parameters, if any
    $ret->{'query-continue'}=\%retc if %retc;

    return $ret;
}

sub _mergequeries {
    my ($to,$from) = @_;

    while(my ($k,$v)=each(%$from)){
        my $r=ref($v);
        next if($r eq 'ARRAY' && @$v == 0);
        if(!exists($to->{$k})){
            $to->{$k}=$v;
        } elsif(ref($to->{$k}) ne $r){
            CORE::warn("\e[31;01mRef mismatch, '".ref($to->{$k})."' ne '$r'!\e[0m");
        } elsif(!$r){
            $to->{$k}=$v;
        } elsif($r eq 'HASH'){
            _mergequeries($to->{$k}, $v);
        } elsif($r eq 'ARRAY'){
            push @{$to->{$k}}, @$v;
        } else {
            CORE::warn("\e[31;01mUnknown ref type '$r'!\e[0m");
        }
    }
}

=pod

=item $api->iterator( key => value, ... )

This function returns an iterator object which iterates over the results of a
query. Use it something like this:

 my $iter = $api->iterator( %q )
 while(my $res = $iter->next()){
     # Do stuff
 }

Note that, in order to be iterable, the query must return exactly one node
under C<< $res->{'query'} >>, which must be an arrayref or a hashref.

In addition, exactly one value in the query may be an arrayref. In this case,
the query will be iterated to completion in turn for each of the values. For
example,

 $api->iterator( titles => ['foo', 'bar', 'baz'], prop => 'info' )

will first query the info for foo, then bar, then baz.

See A<AnomieBOT::API::Iterator> for more information.

=cut

sub iterator {
    return AnomieBOT::API::Iterator->new(@_);
}

=pod

=item $api->paramLimit( $module, $param )

Return the multi-value limit for a parameter.

Returns the limit as an integer, or 0 if there is no limit defined, or an API
error response on error.

=cut

sub paramLimit {
    my ($self, $module, $param) = @_;

    unless ( exists($self->{'paramlimits'}{$module}) ) {
        my $res=$self->_query(action=>'paraminfo', modules=>$module);
        return $res unless $res->{'code'} eq 'success';
        for my $p (@{$res->{'paraminfo'}{'modules'}[0]{'parameters'}}) {
            $self->{'paramlimits'}{$module}{$p->{'name'}} = +$p->{'limit'} if exists($p->{'limit'}) && $p->{'limit'}=~/^\d+$/;
        }
    }
    return $self->{'paramlimits'}{$module}{$param} // 0;
}

=pod

=item $api->login()

=item $api->login( $force )

Try to log the bot in.

Note that the MediaWiki API doesn't actually return an error when the login
fails, but it does return a C<result> property indicating success or failure.
This is translated into a 'notloggedin' error code.

By default, login will return an empty success response if it determines that
the bot is already logged in. You can override this by specifying a
L<true|perlglossary(1)/true> value for C<$force>.

=cut

sub login {
    my ($self, $force) = @_;
    my ($ret);

    return {
        code => 'notconfigured',
        error => 'No password is configured, cannot log in',
    } if $self->{'lgpass'} =~ /^\s*$/;

    if(!$force){
        $self->_throttle('read');
        $ret=$self->_query(action=>'query', meta=>'userinfo', maxlag => 600, __noassert => 1);
        return {
            code => 'success',
            error => 'Already logged in',
        } if($ret->{'code'} eq 'success' && $ret->{'query'}{'userinfo'}{'name'} eq $self->{'lguser'});
    }

    $self->_throttle('read');
    my $r=$self->_query(
        action => 'query',
        meta => 'tokens',
        type => 'login',
        __noassert => 1,
    );
    return $r if($r->{'code'} ne 'success');

    my %q=(
        __nolog    => 1,
        __noassert => 1,
        action     => 'login',
        lgname     => $self->{'lguser'},
        lgpassword => $self->{'lgpass'},
        maxlag     => 600,
    );
    $q{'lgtoken'} = $r->{'query'}{'tokens'}{'logintoken'} if exists( $r->{'query'}{'tokens'}{'logintoken'} );

    $self->_throttle('read');
    for(my $loops=0; $loops<2; $loops++){
        $ret=$self->_query(%q);
        $self->{'ua'}->cookie_jar->save();
        $self->warn("Login failed ($ret->{code})") if($ret->{'code'} ne 'success');
        return $ret if($ret->{'code'} ne 'success');
        return $ret if($ret->{'login'}{'result'} eq 'Success');

        # Not really logged in. Did we get a login token?
        if($ret->{'login'}{'result'} eq 'NeedToken'){
            if(!exists($ret->{'login'}{'token'})){
                $ret->{'code'}='notloggedin';
                $ret->{'error'}='MediaWiki reported NeedToken but did not give us one';
            }
            $q{'lgtoken'}=$ret->{'login'}{'token'};
            redo;
        }

        # Not really logged in. Did MW say to wait?
        my $w=0;
        $w=$ret->{'login'}{'wait'} if exists($ret->{'login'}{'wait'});
        last if $w<=0;

        # Yes they did, do so and try again.
        $self->warn("Login failed (".($ret->{'login'}{'result'} // '"success"').") with a wait time, waiting $w seconds");
        sleep($w);
    }

    # Too many retries failed. Just error out now.
    $ret->{'code'}='notloggedin';
    $ret->{'error'}='MediaWiki reported '.($ret->{'login'}{'result'} // '"success"').', but did not return a login token or a wait time.';
    return $ret;
}

=pod

=item $api->logout()

Log the bot out.

=cut

sub logout {
    my ($self) = @_;
    my ($ret);

    $self->_throttle('read');
    $ret = $self->_query( action => 'query', meta => 'tokens|userinfo', maxlag => 600, __noassert => 1);
    return $ret if $ret->{'code'} ne 'success';
    return {
        code => 'success',
        error => 'Already logged out',
    } if exists( $ret->{'query'}{'userinfo'}{'anon'} );

    $ret = $self->_query( action=>'logout', token => $ret->{'query'}{'tokens'}{'csrftoken'}, __noassert => 1 );
    $self->{'ua'}->cookie_jar->save();
    return $ret;
}

=pod

=item $api->gettoken( $type )

=item $api->gettoken( $type, %options )

Obtain a token of the specified type (see the C<meta=tokens> module). Available
options are:

=over

=item Title

In addition to the token, fetch information for the specified title.

=item Redir

Follow redirects, if C<Title> is provided.

=item NoShutoff

Do not check the shutoff page. Don't do this frivolously.

=item OptOut

If the intention of this edit is to leave a notification on a user's talk page,
set C<Title> and set this to the appropriate token as detailed at
L<en:Template:bots#Message_notification_opt_out>.

=item NoExclusion

Do not check for {{tl|bots}} / {{tl|nobots}} in C<Title>. Don't do this frivolously.

=item links

=item images

=item templates

=item categories

=item [etc]

If specified, the corresponding API C<prop> module will be included in the
query. If the value is a hash reference, the key-value pairs will be
interpreted as the needed parameters for the module.

For example, C<< links => 1, categories => { show => 'hidden' } >> will include
all the page links and the hidden categories in the returned object.

Valid keys are all L<prop modules|mw:API:Properties>.

C<revisions> and C<info> are always queried, with
C<rvprop=ids|timestamp|content|flags|user|size|comment|tags>, C<rvslots=*>, and
C<inprop=protection|talkid|subjectid>.

=back

The object returned will have properties C<code> and C<error> as usual,
C<token> holding the token, C<curtimestamp> holding the timestamp, C<rights>
holding the current user's rights, and if C<Title> was specified the properties
returned for a query of that title.

If the bot is not logged in, C<login(1)> will be automatically attempted; if it
fails, an error code 'notloggedin' will be returned. If the current user cannot
edit the wiki, a 'notallowed' error code will be returned. If the task's
shutoff page (User:I<botname>/shutoff/I<task>) is non-empty, an error code
'shutoff' will be returned.

=cut

sub gettoken {
    my ($self, $toktype, %options) = @_;
    my ($r);

    my $shutoff='User:'.$self->{'lguser'}.'/shutoff/'.$self->{'task'};
    for(my $loops=0; $loops<2; $loops++){
        my %q=(
            meta => 'tokens|userinfo',
            uiprop => 'rights',
            type => $toktype,
            curtimestamp => 1,
        );
        if(!($options{'NoShutoff'} // 0) || exists($options{'Title'})) {
            %q = ( %q,
                prop => 'info|revisions',
                inprop => 'protection|talkid|subjectid',
                rvprop => 'ids|timestamp|content|flags|user|size|comment|tags',
                rvslots => '*',
            );
            $q{'redirects'}=1 if $options{'Redir'} // 0;

            my @titles = ();
            push @titles, $shutoff unless $options{'NoShutoff'} // 0;
            if ( exists($options{'Title'}) ) {
                push @titles, $options{'Title'};
                my $res=$self->loadqueryprops;
                return $res if defined($res);
                my %prop=%{$self->{'queryprops'}};

                my ($k,$v);
                while(my ($p,$n)=each %prop){
                    next unless exists($options{$n});
                    my $opts = $options{$n};
                    $opts = {} unless ref($opts) eq 'HASH';
                    if ( $n eq 'info' ) {
                        while (($k,$v)=each %$opts) {
                            if ($k eq 'inprop') {
                                my %tmp;
                                @tmp{split(/\|/, $q{'inprop'}.'|'.$v)} = ();
                                $q{'inprop'}=join('|', keys %tmp);
                            } else {
                                $q{$k}=$v;
                            }
                        }
                    } elsif ( $n eq 'revisions' ) {
                        while (($k,$v)=each %$opts) {
                            if ($k eq 'rvprop') {
                                my %tmp;
                                @tmp{split(/\|/, $q{'rvprop'}.'|'.$v)} = ();
                                $q{'rvprop'}=join('|', keys %tmp);
                            } else {
                                $q{$k}=$v;
                            }
                        }
                    } else {
                        $q{'prop'}.="|$n";
                        $q{$p."limit"}='max' if exists($self->{'queryparams'}{$n}{'limit'});
                        $q{$p.$k}=$v while(($k,$v)=each %$opts);
                    }
                }
            }
            $q{'titles'} = join( '|', @titles );
        }

        $r=$self->query(%q);
        return $r if($r->{'code'} ne 'success');

        if (exists($r->{'query'}{'userinfo'}{'anon'})) {
            $self->log("Not logged in, attempting to do so");
            $r=$self->login(1);
            return $r if($r->{'code'} ne 'success');
            next;
        }

        if (!exists($r->{'query'}{'tokens'}{"${toktype}token"})) {
            return {
                code  => 'wtferror',
                error => $toktype . ' token was not returned.'
            }
        }
        my $token = $r->{'query'}{'tokens'}{"${toktype}token"};
        my $timestamp = $r->{'query'}{'curtimestamp'} // POSIX::strftime('%FT%TZ', gmtime);

        my %rights=();
        $rights{$_}=$_ foreach (@{$r->{'query'}{'userinfo'}{'rights'}});

        my @r=values(%{$r->{'query'}{'pages'}});

        unless($options{'NoShutoff'}//0) {
            my $sr = undef;
            foreach (@r){ $sr=$_ if $_->{'title'} eq $shutoff; }
            if(!defined($sr)){
                $self->warn('Shutoff token was not returned. WTF?');
                return {
                    code  => 'wtferror',
                    error => 'Shutoff check failed. WTF?'
                };
            }
            if (!defined($self->{'noedit'}) && !exists($sr->{'missing'}) && $sr->{'revisions'}[0]{'slots'}{'main'}{'*'}=~/\S/) {
                return {
                    code => 'shutoff',
                    error => 'Task shutoff',
                    content => $sr->{'revisions'}[0]{'slots'}{'main'}{'*'}
                };
            }
        }

        if (exists($options{'Title'})) {
            my $title = $options{'Title'};
            if(exists($r->{'query'}{'normalized'})){
                foreach (@{$r->{'query'}{'normalized'}}){
                    $title=$_->{'to'} if $_->{'from'} eq $title;
                }
            }
            if(exists($r->{'query'}{'redirects'})){
                foreach (@{$r->{'query'}{'redirects'}}){
                    $title=$_->{'to'} if $_->{'from'} eq $title;
                }
            }

            $r = undef;
            foreach (@r){ $r=$_ if $_->{'title'} eq $title; }
            if(!defined($r)){
                $self->warn('Data for title "'.$title.'" (normalized from "'.$options{'Title'}.'") was not returned. WTF?');
                return {
                    code  => 'wtferror',
                    error => 'Data for title "'.$title.'" (normalized from "'.$options{'Title'}.'") was not returned.'
                };
            }

            if(exists($r->{'invalid'})){
                $self->warn("Invalid title $title was queried!");
                return {
                    code  => 'invalidtitle',
                    error => "Bad title ``$title''",
                };
            }

            # Check bot exclusion
            if(!exists($r->{'missing'}) && !(exists($options{'NoExclusion'}) && $options{'NoExclusion'})){
                my $deny='';
                my $type='';
                {
                    my $x;
                    my $c=$r->{'revisions'}[0]{'slots'}{'main'}{'*'};
                    if(!defined($c)){
                        $self->warn("Page contents missing for $title, probably a MediaWiki:Missing-article error.");
                        return {
                            code  => 'wtferror',
                            error => "Page contents missing for $title.",
                        };
                    }
                    $c=~s{<(nowiki|pre|source)(?:\s[^>]*)?(?:/>|(?<!/)>.*?(?:</\g{-1}(?:\s*)>|$))|<!--.*?(?:-->|$)}{}siog;
                    if($c=~/\{\{[nN]obots\}\}/){ $deny='{{'.'nobots}}'; $type='bydefault'; last; }
                    if($c=~/\{\{[bB]ots\}\}/){ $deny=''; last; }
                    if($c=~/\{\{[bB]ots\s*\|\s*allow\s*=\s*(.*?)\s*\}\}/s){
                        if($1 eq 'all'){ $deny=''; last; }
                        if($1 eq 'none'){ $deny='{{'.'bots|allow=none}}'; $type='bydefault'; last; }
                        unless(grep { $_ eq $self->{'lguser'} } split(/\s*,\s*/, $1)){
                            $deny='{{'.'bots|allow=...}} without '.$self->{'lguser'};
                            $type='bydefault';
                        }
                        last;
                    }
                    if($c=~/\{\{[bB]ots\s*\|\s*deny\s*=\s*(.*?)\s*\}\}/s){
                        if($1 eq 'all'){ $deny='{{'.'bots|deny=all}}'; $type='bydefault'; last; }
                        if($1 eq 'none'){ $deny=''; last; }
                        if(grep { $_ eq $self->{'lguser'} } split(/\s*,\s*/, $1)){
                            $deny='{{'.'bots|deny=...}} with '.$self->{'lguser'};
                            $type='byname';
                        }
                        last;
                    }
                    if(exists($options{'OptOut'}) && $c=~/\{\{[bB]ots\s*\|\s*optout\s*=\s*(.*?)\s*\}\}/s){
                        if($1 eq 'all'){ $deny='{{bots|optout=all}}'; $type='optout'; last; }
                        if(grep { $_ eq $options{'OptOut'} } split(/\s*,\s*/, $1)){
                            $deny="{{bots|optout=...}} with ".$options{'OptOut'};
                            $type='optout';
                        }
                        last;
                    }
                }
                if($deny ne ''){
                    $r->{'code'}='botexcluded';
                    $r->{'error'}="Found $deny";
                    $r->{'type'}=$type;
                    delete($r->{$toktype.'token'});
                    return $r;
                }
            }
        } else {
            $r = {};
        }

        $r->{'token'} = $token;
        $r->{'curtimestamp'} = $timestamp;
        $r->{'rights'}=\%rights;
        $r->{'code'}='success';
        $r->{'error'}='Success';
        $r->{'self'}=$self;
        return $r;
    }

    # Too many retries failed. Just error out now.
    $r->{'code'}='wtferror';
    $r->{'error'}='Login seems to succeed but we\'re still anon. WTF?';
    return $r;
}

=pod

=item $api->edittoken( $title )

=item $api->edittoken( $title, %options )

Obtain an edit token for the specified page. This is much like C<gettoken()>,
with the following differences:

=over

=item *

C<Title> is always passed.

=item *

C<Redir> is set, unless C<EditRedir> is true.

=back

In addition to the options available for C<gettoken()>, the following are
available:

=over

=item EditRedir

C<Redir> is defaulted to true. This causes C<Redir> to be set false.

=back

The object returned here must be passed to C<edit()>.

=cut

sub edittoken {
    my ($self, $title, %options) = @_;

    $options{'Redir'}=1 unless $options{'EditRedir'} // 0;
    $options{'Title'}=$title;

    my $tok=$self->gettoken('csrf', %options);
    return $tok unless $tok->{'code'} eq 'success';

    # Check page protection
    my %rights = %{$tok->{'rights'}};
    my $permfail='';
    if(!exists($tok->{'missing'})){
        $permfail='edit' unless exists($rights{'edit'});
    } elsif($tok->{'ns'}&1){
        $permfail='createtalk' unless exists($rights{'createtalk'});
    } else {
        $permfail='createpage' unless exists($rights{'createpage'});
    }
    if(exists($tok->{'protection'})){
        foreach (@{$tok->{'protection'}}){
            if(($_->{'type'} eq 'create' && exists($tok->{'missing'})) ||
               ($_->{'type'} eq 'edit' && !exists($tok->{'missing'}))){
                $_->{'level'}='protect' if($_->{'level'} eq 'sysop');
                $permfail=$_->{'level'} unless(exists($rights{$_->{'level'}}) || (exists($rights{'editprotected'}) && !exists($_->{'cascade'})));
            }
        }
    }
    if($permfail ne ''){
        $tok->{'code'}='pageprotected';
        $tok->{'error'}="Editing this page requires the $permfail permission";
        delete($tok->{'token'});
        return $tok;
    }
    return $tok;
}

=pod

=item $api->edit( $token, $text, $summary, $minor, $bot, %params )

Perform an edit to the page.

Note that the default configuration uses the C<assert> parameter to assert that
the current user has the "bot" flag. This means that the edit will fail if your
bot is not flagged; the AnomieBOT A<trial.pl> script overrides this default, so
bot trials may still be done.

=cut

sub edit {
    my ($self, $token, $text, $summary, $minor, $bot, %params) = @_;
    if(ref($token) ne 'HASH' || $token->{'self'} ne $self){
        $self->warn("Invalid token");
        return {
            code  => 'params',
            error => 'Invalid $token'
        };
    }
    my %param=(
        action  => 'edit',
        title   => $token->{'title'},
        text    => $text,
        token   => $token->{'token'},
        summary => $summary,
        md5     => md5_hex($md5_need_encode?encode('UTF-8',$text):$text),
        starttimestamp => _ISO2wptime($token->{'curtimestamp'}),
        watchlist => $self->{'edit_watchlist'},
    );
    $param{$minor?'minor':'notminor'}=1 if defined($minor);
    $param{'bot'}=1 if(defined($bot) && $bot);
    if(exists($token->{'missing'})){
        $param{'basetimestamp'}=_ISO2wptime($token->{'curtimestamp'});
        $param{'createonly'}=1;
    } else {
        $param{'basetimestamp'}=_ISO2wptime($token->{'revisions'}[0]{'timestamp'});
        $param{'nocreate'}=1;
    }
    $param{'assert'}=$self->{'assert_edit'} if($self->{'assert_edit'} ne '');
    $param{'nassert'}=$self->{'nassert_edit'} if($self->{'nassert_edit'} ne '');

    %param = ( %param, %params ) if %params;

    if(defined($self->{'editlimit'}) && $self->{'editlimit'}<=0){
        die "Edit limit reached, bot halting.";
    }
    if(defined($self->{'noedit'})){
        # Fake edit
        my $t=$param{'title'}.'<'.(exists($token->{'missing'})?'new':$token->{'lastrevid'}).POSIX::strftime('>%FT%TZ.txt', gmtime);
        $t=~s! !_!g;
        $t=~s!/!#!g;
        $t=$self->{'noedit'}.'/'.$t;
        open(X, ">:utf8", $t) or die("Could not open $t: $!\n");
        print X $text;
        close(X);
        print "\e[34mEDIT to ".$param{'title'}." ($summary): $t\e[0m\n";
        return {
            code  => 'success',
            error => 'Success',
            edit  => {
                oldrevid => $token->{'lastrevid'},
                newrevid => $token->{'lastrevid'},
                pageid   => $token->{'pageid'},
                title    => $token->{'title'},
                result   => 'Success'
            }
        };
    }
    $self->_throttle('edit');
    my $res=$self->_query(%param);
    if($res->{'code'} eq 'success'){
        # The edit API might return failure in a different way
        if(lc($res->{'edit'}{'result'}) eq 'success'){
            $self->{'editlimit'}-- if defined($self->{'editlimit'});
            return $res;
        }
        $res->{'code'}=$res->{'edit'}{'result'};
        $res->{'error'}='Edit hook error';

        # Well-behaved extensions will include an explanation token in the
        # result. Pull out some common ones.
        if(exists($res->{'edit'}{'spamblacklist'})){
            $res->{'error'}.=': Spam blacklist triggered on "'.$res->{'edit'}{'spamblacklist'}.'"';
        } elsif(exists($res->{'edit'}{'assert'})){
            $res->{'error'}.=': Assertion "'.$res->{'edit'}{'assert'}.'" failed';
        } elsif(exists($res->{'edit'}{'nassert'})){
            $res->{'error'}.=': Negative assertion "'.$res->{'edit'}{'nassert'}.'" failed (i.e. the condition passed)';
        } elsif(exists($res->{'edit'}{'captcha'})){
            $res->{'error'}.=': Captcha required';
        } elsif(exists($res->{'edit'}{'info'}) && ref($res->{'edit'}{'info'}) eq ""){
            # Probably AbuseFilter
            $res->{'error'}.=': ' . $res->{'edit'}{'info'};
            $res->{'error'}.=' (' . $res->{'edit'}{'code'} . ')' if exists($res->{'edit'}{'code'});
        } else {
            # Something unknown. Just tack on the whole response object.
            my $x=Dumper($res->{'edit'});
            $x=~s/\n\s*/ /g;
            $res->{'error'}.=": $x";
        }
    }
    carp "Edit error: ".$res->{'error'};
    return $res;
}

=pod

=item $api->upload( $token, %options )

Upload a file.

The C<$token> must be obtained from C<< $api->gettoken >> with the target file
name passed as C<Title>. To specify the file contents, exactly one of the
following options must be given:

=over

=item Url

Url that the MediaWiki server should download the file from.

=item HttpStatus

Do not actually upload anything, just return the status of the upload
corresponding to the session key specified here.

=item FileKey

If the initial upload returned warnings (or C<Stash> was used), specify the
returned file key here to complete the upload. C<SessionKey> is accepted as an
alias for backwards compatability.

=item File

Filename on the local system to upload, accessible to the bot. Note the file
will be read in binary (L<:raw|PerlIO(3perl)/:raw>) mode.

=item Handle

Open file handle from which the data to upload will be read.

=item Data

Raw file data to upload.

=back

Additional options are:

=over

=item Comment

Comment for the upload; note that MediaWiki will also use this for the initial
file page text if the file does not already exist and C<Text> is not used.

=item Text

Initial file page text if the file does not already exist, rather than using
C<Comment>.

=item AsyncDownload

When using Url, setting a true value here tells MediaWiki to return us a
session key immediately (which may be passed to HttpStatus in a later call)
rather than waiting for the download to actually complete.

=item IgnoreWarnings

Ignore any warnings.

=item Stash

Stash file temporarily.

=back

=cut

sub upload {
    my ($self, $token, %options) = @_;
    if(ref($token) ne 'HASH' || $token->{'self'} ne $self){
        $self->warn("Invalid token");
        return {
            code  => 'params',
            error => 'Invalid $token'
        };
    }
    if(($token->{'ns'}//-1) ne 6){
        $self->warn("Token is not for a title in the File namespace.");
        return {
            code  => 'params',
            error => 'Token is not for the File namespace.'
        };
    }

    if ( exists($options{'SessionKey'}) ) {
        $options{'FileKey'} = $options{'SessionKey'} unless exists( $options{'FileKey'} );
        delete $options{'SessionKey'};
    }

    my @req=qw/Url HttpStatus FileKey File Handle Data/;
    my $ct=0;
    foreach (@req){
        $ct++ if exists($options{$_});
    }
    if($ct!=1){
        $req[$#req]='or '.$req[$#req];
        return {
            code  => 'params',
            error => 'Exactly one of '.join(', ', @req).' must be specified.'
        };
    }

    my %param=(
        action    => 'upload',
        token     => $token->{'token'},
        watchlist => $self->{'edit_watchlist'},
    );
    ($param{'filename'}=$token->{'title'})=~s/^[^:]*://;
    $param{'comment'}=$options{'Comment'} if exists($options{'Comment'});
    $param{'text'}=$options{'Text'} if exists($options{'Text'});
    $param{'asyncdownload'}=1 if($options{'AsyncDownload'} // 0);
    $param{'ignorewarnings'}=1 if($options{'IgnoreWarnings'} // 0);
    $param{'stash'}=1 if($options{'Stash'} // 0);

    if(exists($options{'Url'})){
        $param{'url'}=$options{'Url'};
    } elsif(exists($options{'HttpStatus'})){
        $param{'httpstatus'}=1;
        $param{'filekey'}=$options{'HttpStatus'};
    } elsif(exists($options{'FileKey'})){
        $param{'filekey'}=$options{'FileKey'};
    } elsif(exists($options{'File'})){
        $param{'file'}=[ $options{'File'}, $param{'filename'} ];
    } elsif(exists($options{'Handle'})){
        {
            local $/=undef;
            my $fh=$options{'Handle'};
            $param{'file'}=[ undef, $param{'filename'}, Content => scalar <$fh> ];
        }
    } elsif(exists($options{'Data'})){
        $param{'file'}=[ undef, $param{'filename'}, Content => $options{'Data'} ];
    }

    if(defined($self->{'editlimit'}) && $self->{'editlimit'}<=0){
        die "Edit limit reached, bot halting.";
    }
    if(defined($self->{'noedit'})){
        # Fake edit
        if(exists($param{'file'})){
            my $t='Upload##'.$param{'filename'}.'<'.(exists($token->{'missing'})?'new':$token->{'lastrevid'}).POSIX::strftime('>%FT%TZ.txt', gmtime);
            $t=~s! !_!g;
            $t=~s!/!#!g;
            $t=$self->{'noedit'}.'/'.$t;
            open(X, ">:raw", $t) or die("Could not open $t: $!\n");
            if(defined($param{'file'}[0])){{
                local $/=undef;
                open(XX, "<:raw", $param{'file'}[0]);
                print X scalar <XX>;
                close XX;
            }} else {
                print X $param{'file'}[3];
            }
            close(X);
        } elsif(exists($param{'url'})){
            print "\e[34mUPLOAD from ".$param{'url'}."\e[0m\n";
        }
        if(exists($token->{'missing'})){
            my $t='File:'.$param{'filename'}.'<new'.POSIX::strftime('>%FT%TZ.txt', gmtime);
            $t=~s! !_!g;
            $t=~s!/!#!g;
            $t=$self->{'noedit'}.'/'.$t;
            open(X, ">:utf8", $t) or die("Could not open $t: $!\n");
            print X $param{'comment'}//'';
            close(X);
        }
        print "\e[34mUPLOAD to ".$param{'filename'}."\e[0m\n";
        return {
            code   => 'success',
            error  => 'Success',
            upload => {
                result    => 'Success'
                # XXX: Fake up the rest somehow?
            }
        };
    }
    $self->_throttle('edit');
    my $res=$self->_query(%param);
    if($res->{'code'} eq 'success'){
        # The upload API might return failure in a different way
        if(lc($res->{'upload'}{'result'} // 'Success') eq 'success'){
            $self->{'editlimit'}-- if defined($self->{'editlimit'});
            return $res;
        }
        if(lc($res->{'upload'}{'result'}) eq 'warning'){
            $res->{'code'}='uploadwarnings';
            $res->{'error'}="Upload warnings: ".join(', ', keys %{$res->{'upload'}{'warnings'}});
            return $res;
        }
        $res->{'code'}=$res->{'upload'}{'error'};
        $res->{'error'}="Upload error: ".$res->{'code'};
    }
    carp "Upload error: ".$res->{'error'};
    return $res;
}

=pod

=item $api->movetoken( $title )

=item $api->movetoken( $title, %options )

Obtain a move token for the specified page. Options are:

=over

=item EditRedir

Move the redirect page, instead of the page it points to.

=item NoShutoff

Do not check the shutoff page. Don't do this frivolously.

=item NoExclusion

Do not check for {{tl|bots}} / {{tl|nobots}}. Don't do this frivolously.

=back

The object returned here must be passed to C<move()>. The object contains the
same properties as that returned by C<edittoken()>, plus the following:

=over

=item can_suppressredirect

True if the C<$noredirect> parameter to C<move()> will be honored (i.e. the
user has the 'suppressredirect' right).

=back

=cut

sub movetoken {
    my ($self, $title, %options) = @_;
    my %rights=();

    delete $options{'OptOut'};
    $options{'Title'}=$title;
    my $tok=$self->gettoken('csrf', %options);
    return $tok unless $tok->{'code'} eq 'success';

    if(exists($tok->{'missing'})){
        $tok->{'code'}='pagemissing';
        $tok->{'error'}="Cannot move a nonexistent page";
        delete($tok->{'token'});
        return $tok;
    }

    # Check page protection
    my $permfail='';
    my $rights = $tok->{'rights'};
    if(!exists($rights{'move'})){
        $permfail='move';
    } elsif($tok->{'ns'}==2 && index($tok->{'title'},'/')<0){
        $permfail='move-rootuserpages' unless exists($rights{'move-rootuserpages'});
    } elsif($tok->{'ns'}==6){
        $permfail='movefile' unless exists($rights{'movefile'});
    }
    if(exists($tok->{'protection'})){
        foreach (@{$tok->{'protection'}}){
            if($_->{'type'} eq 'move'){
                $_->{'level'}='protect' if($_->{'level'} eq 'sysop');
                $permfail=$_->{'level'} unless exists($rights{$_->{'level'}});
            }
        }
    }
    if($permfail ne ''){
        $tok->{'code'}='pageprotected';
        $tok->{'error'}="Moving this page requires the $permfail permission";
        delete($tok->{'token'});
        return $tok;
    }
    $tok->{'can_suppressredirect'}=exists($rights{'suppressredirect'});
    return $tok;
}

=pod

=item $api->move( $token, $totitle, $reason, $movetalk, $noredirect )

Move the page to C<$totitle>.

=cut

sub move {
    my ($self, $token, $totitle, $reason, $movetalk, $noredirect) = @_;
    if(ref($token) ne 'HASH' || $token->{'self'} ne $self){
        $self->warn("Invalid token");
        return {
            code  => 'params',
            error => 'Invalid $token'
        };
    }
    my %param=(
        action    => 'move',
        from      => $token->{'title'},
        to        => $totitle,
        token     => $token->{'token'},
        reason    => $reason,
        watchlist => $self->{'edit_watchlist'},
    );
    $param{'movetalk'}=1 if(defined($movetalk) && $movetalk);
    $param{'noredirect'}=1 if(defined($noredirect) && $noredirect);

    if(defined($self->{'editlimit'}) && $self->{'editlimit'}<=0){
        die "Edit limit reached, bot halting.";
    }
    if(defined($self->{'noedit'})){
        # Fake edit
        print "\e[34mMOVE from ".$param{'from'}." to ".$param{'to'}." ($reason)\e[0m\n";
        my $ret={
            from     => $token->{'title'},
            to       => $totitle,
            reason   => $reason,
        };
        $ret->{'redirectcreated'}='' unless($noredirect && exists($token->{'can_suppressredirect'}));
        return {
            code  => 'success',
            error => 'Success',
            move  => $ret
        };
    }
    $self->_throttle('edit');
    my $res=$self->_query(%param);
    if($res->{'code'} eq 'success'){
        $self->{'editlimit'}-- if defined($self->{'editlimit'});
        return $res;
    }
    carp "Move error: ".$res->{'error'};
    return $res;
}

=pod

=item $api->action( $token, %param )

Perform an action that doesn't have a custom function. C<%param> must contain
an "action" key, as well as any other keys needed for the action ("token" may
be omitted).

=cut

sub action {
    my ($self, $token, %param) = @_;
    if(ref($token) ne 'HASH' || $token->{'self'} ne $self){
        $self->warn("Invalid token");
        return {
            code  => 'params',
            error => 'Invalid $token'
        };
    }
    unless(exists($param{'action'})){
        $self->warn("Missing action");
        return {
            code  => 'params',
            error => 'Missing action'
        };
    }
    $param{'token'} //= $token->{'token'};

    if(defined($self->{'editlimit'}) && $self->{'editlimit'}<=0){
        die "Edit limit reached, bot halting.";
    }
    my $act = uc( $param{'action'} );
    if(defined($self->{'noedit'})){
        # Fake edit
        my $title = $token->{'title'}//'<no title>';
        print "\e[34m$act on $title\e[0m\n" . Dumper( \%param ) . "\n";
        return {
            code  => 'success',
            error => 'Success',
        };
    }
    $self->_throttle('edit');
    my $res=$self->_query(%param);
    if($res->{'code'} eq 'success'){
        $self->{'editlimit'}-- if defined($self->{'editlimit'});
        return $res;
    }
    carp "$act error: ".$res->{'error'};
    return $res;
}

sub DESTROY {
    my $self=shift;
    untie %{$self->{'storehash'}};
    delete $self->{'storehash'};
    delete $self->{'store'};
}


# Utility funcs

sub _handle_maxlag {
    my $self=shift;
    my $res=shift;
    my $reason=shift;
    my $delay=10;

    my $header=$res->header('Retry-After') // '';
    $header=~s/^\s+|\s+$//g;
    my $t;
    if($header=~/^\d+$/){
        $delay=$header;
    } elsif(defined($t=str2time($header))){
        $delay=POSIX::ceil($t-time());
        $delay=1 if $delay<1;
    }
    if(!$self->automaxlag){
        return {
            code     => 'maxlag',
            error    => $reason,
            delay    => $delay,
        };
    }
    carp POSIX::strftime('[%F %T] ', localtime).$self->{'task'}.' ('.$self->{'botnum'}.'): Pausing for maxlag ('.$delay.'s): '.$reason;
    while($delay>10){
        sleep 10;
        $delay-=10;
        return {
            code     => 'maxlag',
            error    => $reason,
            delay    => $delay,
        } if !$self->automaxlag;
    }
    sleep $delay;
    return undef;
}

use vars '$AUTOLOAD';
sub AUTOLOAD {
    my $func=$AUTOLOAD;
    $func=~s/.*://;
    if($func!~/^_/ && @_ && ref($_[0]) && $_[0]->isa('AnomieBOT::API')){
        my $self=$_[0];
        foreach my $d (@{$self->{'decorators'}}){
            my $sub=$d->can($func);
            goto &$sub if defined($sub);
        }
    }
    croak "Undefined subroutine &$AUTOLOAD called";
}

=pod

=back

=head1 UTILITY METHODS

=over

=item AnomieBOT::API::load( $file )

Load the task contained in the specified file, if it hasn't already been
loaded.

=cut

my %loaded=();
sub load {
    my $m=$_[0];
    if($m=~/::/){
        $m=~s/::/\//g;
        $m.='.pm';
    }
    my $file=realpath($m);
    croak "File not found: $m" unless defined($file);
    eval {
        require $file unless exists($loaded{$file});
    };
    croak "Could not load $file: $@" if $@;
    $loaded{$file}=1;
}

=pod

=item $AnomieBOT::API::basedir

Returns the base directory for the bot.

Specifically, this returns the directory that contains A<AnomieBOT/API.pm>.
Note this may croak if you loaded A<AnomieBOT::API> in some other way than
C<require AnomieBOT::API;> or C<use AnomieBOT::API;>.

=cut

$AnomieBOT::API::basedir=undef;
croak 'AnomieBOT::API not found in %INC' unless exists($INC{'AnomieBOT/API.pm'});
my $basedir=realpath($INC{'AnomieBOT/API.pm'});
croak $INC{'AnomieBOT/API.pm'}.' could not be found; did you chdir after loading it?' unless -e $basedir;
$AnomieBOT::API::basedir=substr($basedir, 0, -17);

1;

=pod

=back

=head1 COPYRIGHT

Copyright 2008–2013 Anomie

This library is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.
