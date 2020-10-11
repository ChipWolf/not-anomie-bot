package AnomieBOT::API::Cache::Memcached;
use parent AnomieBOT::API::Cache::Encrypted;

use utf8;
use strict;

use Data::Dumper;
use Carp;
use Socket;
use IO::Socket;
use Digest::CRC ('crc32');

=pod

=head1 NAME

AnomieBOT::API::Cache::Memcached - AnomieBOT API cache using memcached

=head1 SYNOPSIS

 use AnomieBOT::API::Cache;

 my $cache = AnomieBOT::API::Cache->create( 'Memcached', $optionString );
 $cache->set( 'foo', 'bar' );
 say $cache->get( 'foo' );  # Outputs "bar"

=head1 DESCRIPTION

C<AnomieBOT::API::Cache::Memcached> is an implementation of
A<AnomieBOT::API::Cache> using memcached for storage.

=head1 METHODS

In addition to the methods inherited from the base class, the following are available.

=over

=item AnomieBOT::API::Cache::Memcached->new( $optionString )

Creates a new AnomieBOT::API::Cache::Memcached object. The option string is a
semicolon-separated list of key-value pairs; if the value must contain a
semicolon or backslash, escape it using a backslash.

Recognized keys are:

=over

=item servers

Comma-separated list of server addresses, and optional weights. Each server
address is of the form "host:port" for network connections, or
"/path/to/socket" for Unix domain socket connections. An integer weight may be
specified by appending "!weight".

=item namespace

Prefix all keys with this string.

=item connect_timeout

Seconds to wait before considering a connection attempt has failed. Default 0.25 seconds.

=item io_timeout

Seconds to wait before considering a read or write attempt has failed. Default 1 second.

=item max_size

Maximum size of a data item, after compression. Larger data items will cause
setting functions to return undef. Set 0 to disable. Default is 0.

=item encrypt

Encrypts the data before sending it to memcached, using the specified value as
the encryption key. Default is empty, no encryption.

=item user

=item pass

Username and password to send as an "authenticate" command, for sharp-memcached
used in Tool Forge.

=item verbose

Output errors to stdout.

=back

=cut

sub new {
    my ($class, $optionString) = @_;
    my %opts = $class->explode_option_string( $optionString );

    croak "$class requires one or more servers\n" unless '' ne ($opts{'servers'}//'');
    my @servers=();
    foreach my $s (split /,/, $opts{'servers'}){
        my ($weight,$sa,$af,$p)=(1,undef,undef,0);
        ($s,$weight)=($1,$2) if $s=~/^(.+)!(\d+)$/;
        if($s=~m!/!){
            ($sa,$af,$p)=(Socket::pack_sockaddr_un($s), PF_UNIX, 0);
        } else {
            my ($ip,$port)=($s,11211);
            my $enc;
            ($ip,$port)=($1,$2) if $s=~/^(.+):(\d+)$/;
            $ip=$1 if $ip=~/^\[(.*)\]$/; # Strip IPv6 brackets
            my ($ok, $res) = Socket::getaddrinfo($ip, $port, { protocol => scalar(getprotobyname("tcp")) });
            ($sa,$af,$p) = @{$res}{'addr','family','protocol'} if $ok eq '';
        }
        croak "Invalid server address '$s'" unless $sa;
        my $info=[$s,$sa,$af,$p,"$af/$p/$sa"];
        push @servers, $info while($weight--);
    }

    my $oldself = $class->SUPER::new($optionString);
    my $self = {
        %$oldself,
        servers => \@servers,
        num_servers => scalar(@servers),
        socks => {},
        socknames => {},
        namespace => $opts{'namespace'}//'',
        connect_timeout => $opts{'connect_timeout'}//0.25,
        io_timeout => $opts{'io_timeout'}//1.0,
        max_size => $opts{'max_size'}//0,
        user => $opts{'user'}//'',
        pass => $opts{'pass'}//'',
        verbose => $opts{'verbose'}//0,
        rdbuf => '',
    };

    bless $self, $class;
    return $self;
}

=pod

=item $cache->socket_for_key( $key )

Return the socket for the server used for a particular key. In a list context,
also returns the internally munged key.

=item $cache->all_sockets()

Return sockets for all the servers.

=cut

# Get a socket, by index into $self->{'servers'}
# $cache->_get_socket( $index )
sub _get_socket {
    my ($self,$n)=@_;
    my ($s,$sa,$af,$p,$k)=@{$self->{'servers'}[$n]};
    if(!exists($self->{'socks'}{$k})){
        my $sock = new IO::Socket;
        $sock->socket($af, SOCK_STREAM, $p);
        $sock->timeout($self->{'connect_timeout'});
        if($sock->connect($sa)){
            $sock->blocking(0);
            $sock->autoflush(1);
            $self->{'socks'}{$k}=$sock;
            $self->{'socks'}{$sock}=$k;
            $self->{'socknames'}{$k}=$s;
            $self->{'socknames'}{$sock}=$s;
            if(!$self->_authenticate($sock)){
                $self->_rm_socket($sock, "Authentication to %s failed: $@");
            }
        } else {
            $@="Connect to $s failed: $!";
            carp "$@\n" if $self->{'verbose'};
        }
    }
    return $self->{'socks'}{$k} // undef;
}

# Remove a socket, optionally setting an error message
# $cache->_rm_socket( $sock, $err )
sub _rm_socket {
    my ($self,$k,$err) = @_;

    if($err){
        my $n=$self->{'socknames'}{$k}//'<unknown>';
        $err=~s/%s/$n/g;
    }
    if(exists($self->{'socks'}{$k})){
        my $k2=$self->{'socks'}{$k};
        delete $self->{'socks'}{$k};
        delete $self->{'socks'}{$k2};
        delete $self->{'socknames'}{$k};
        delete $self->{'socknames'}{$k2};
    }
    eval { $k->close(); };
    $@=$err if $err;
    carp $err if $err && $self->{'verbose'};
}

sub socket_for_key {
    my ($self, $key) = @_;

    my $mk = $self->munge_key($key) // return undef, undef;
    my $n;
    if($self->{'num_servers'}==1){
        $n=0;
    } else {
        # Hash the key to choose a server. There are better ways, but this
        # works for now.
        $n = crc32($mk) % $self->{'num_servers'};
    }
    my $sock = _get_socket($self, $n);
    return wantarray ? ($sock,$mk) : $sock;
}

sub all_sockets {
    my ($self) = @_;
    my $l=$self->{'num_sockets'};
    for(my ($i,$l)=(0,$self->{'num_sockets'}); $i<$l; $i++){
        _get_socket($self, $i);
    }
    return values(%{$self->{'socks'}});
}


=pod

=item $cache->command( $sock, $cmd, $read_reply )

Send a command to memcached. The command must include the terminal "\r\n", and
may include binary data if applicable.

Returns undef on error. On success, returns the (first) response line if
C<$read_reply> is true, or a true value otherwise.

=cut

sub command {
    my ($self, $sock, $cmd, $read_reply) = @_;

    local $SIG{'PIPE'} = "IGNORE";
    my ($in, $out, $n) = ('', '');
    my ($o,$l) = (0, length($cmd));
    vec($in, $sock->fileno, 1) = 1;
    while($o<$l){
        $n = select(undef, $out=$in, undef, $self->{'io_timeout'});
        if(!$n){
            # Crap, died.
            $self->_rm_socket($sock, "Write to %s timed out");
            return undef;
        }
        $n = $sock->syswrite( $cmd, $l-$o, $o );
        if(!defined($n)){
            $self->_rm_socket($sock, "Write to %s failed: $!");
            return undef;
        }
        $o+=$n;
    }
    return $read_reply ? $self->read_response($sock) : 1;
}

=pod

=item $cache->read_response( $sock )

Read a response line from memcached. The trailing "\r\n" is stripped.

Returns the line on success, or undef on error.

=cut

sub read_response {
    my ($self, $sock) = @_;

    local $SIG{'PIPE'} = "IGNORE";
    my ($in, $out, $n, $e) = ('', '');
    vec($in, $sock->fileno, 1) = 1;
    while(($e=index($self->{'rdbuf'},"\r\n"))<0){
        $n = select($out=$in, undef, undef, $self->{'io_timeout'});
        if(!$n){
            # Crap, died.
            $self->_rm_socket($sock, "Read from %s timed out");
            return undef;
        }
        $n = $sock->sysread( $self->{'rdbuf'}, 1024, length($self->{'rdbuf'}) );
        if(!defined($n)){
            $self->_rm_socket($sock, "Read from %s failed: $!");
            return undef;
        }
        if(!$n){
            $self->_rm_socket($sock, "Unexpected EOF from %s");
            return undef;
        }
    }
    my $ret = substr($self->{'rdbuf'}, 0, $e);
    substr($self->{'rdbuf'}, 0, $e+2) = '';
    return $ret;
}

=pod

=item $cache->read_data( $sock, $length )

Read binary response data from memcached. C<$length> is the length in bytes to
read, not including the trailing "\r\n". The trailing "\r\n" is stripped.

Returns the binary data on success, or undef on error.

=cut

sub read_data {
    my ($self, $sock, $bytes) = @_;
    my ($in, $out, $n, $e) = ('', '');
    vec($in, $sock->fileno, 1) = 1;
    while(length($self->{'rdbuf'})<$bytes+2){
        $n = select($out=$in, undef, undef, $self->{'io_timeout'});
        if(!$n){
            # Crap, died.
            $self->_rm_socket($sock, "Read from %s timed out");
            return undef;
        }
        $n = $sock->sysread( $self->{'rdbuf'}, $bytes+2-length($self->{'rdbuf'}), length($self->{'rdbuf'}) );
        if(!defined($n)){
            $self->_rm_socket($sock, "Read from %s failed: $!");
            return undef;
        }
        if(!$n){
            $self->_rm_socket($sock, "Unexpected EOF from %s");
            return undef;
        }
    }
    if(substr($self->{'rdbuf'}, $bytes, 2) ne "\r\n"){
        $self->_rm_socket($sock, "Incorrect raw data length reading from %s");
        return undef;
    }
    my $ret = substr($self->{'rdbuf'}, 0, $bytes);
    substr($self->{'rdbuf'}, 0, $bytes+2) = '';
    return $ret;
}


# Send the "authenticate" command, if applicable. Returns boolean.
sub _authenticate {
    my ($self, $sock) = @_;

    return 1 if $self->{'user'} eq '';

    my $res = $self->command( $sock, "authenticate $self->{user}:$self->{pass}\r\n", 1 ) // return undef;
    return 1 if $res eq 'SUCCESS';
    if($res=~s/^ERROR\d*\s+//){
        $@ = $res;
    } else {
        $@ = "Invalid response: $res";
    }
    return 0;
}


sub _get {
    my ($cmd, $self, @keys) = @_;

    croak "At least one key must be given" if @keys<1;

    my %socks = ();
    my %keymap = ();
    for my $k (@keys) {
        my ($sock, $mk) = $self->socket_for_key($k);
        if($sock){
            $keymap{$mk}=$k;
            $socks{$sock} //= [$sock];
            push @{$socks{$sock}}, $mk;
        }
    }

    my %ret = ();
    my %tokens = ();
    for my $a (values %socks){
        my $sock = shift @$a;
        my @delete = ();
        next unless $self->command($sock, "$cmd ".join(' ', @$a)."\r\n", 0);
        while(1){
            my $r = $self->read_response($sock) // last;
            last if $r eq 'END';
            if($r =~ s/^ERROR\d*\s+//){
                $@ = "Memcached $cmd failed: $r";
                carp "$@\n" if $self->{'verbose'};
                next;
            }
            unless($r =~ /^VALUE (\S+) (\d+) (\d+)(?: (\d+))?$/){
                $self->_rm_socket( $sock, "Invalid response to $cmd from %s: $r" );
                last;
            }
            my ($mk, $flags, $bytes, $token) = ($1, $2, $3, $4);
            my $data = $self->read_data($sock, $bytes) // last;
            my $key = $keymap{$mk} // next;
            $data = $self->decode_data($key, $data, $flags);
            if(defined($data)){
                $ret{$key} = $data;
                $tokens{$key} = $token;
            } else {
                push @delete, $mk;
            }
        }
        while(@delete && $sock->connected){
            $self->command($sock, "delete ".shift(@delete)." noreply\r\n", 0);
        }
    }

    if(@keys == 1){
        return $ret{$keys[0]} // undef, $tokens{$keys[0]} // undef;
    } else {
        return \%ret, \%tokens;
    }
}

sub get {
    my ($ret) = _get( 'get', @_ );
    return $ret;
}

sub gets {
    return _get( 'gets', @_ );
}


sub _set {
    my $noreply = defined(wantarray) ? '' : ' noreply';
    my $cmd = shift;
    my $self = shift;
    my $hash = shift;
    my $one = '';
    if(!ref($hash)){
        $one = $hash;
        $hash = { $hash => shift };
    }
    my $tokens = {};
    my $fmt = '%s %s %d %d %d';
    if($cmd eq 'cas' ){
        $fmt .= ' %s';
        $tokens = shift;
        croak "When passing a hashref of key-value pairs, you must also pass a hashref of cas tokens" if $one eq '' and !ref($tokens);
        croak "When passing a single key-value pair, you must also pass a single cas token (not a hashref)" if $one ne '' and ref($tokens);
        $tokens = { $one => $tokens } if $one ne '';
    }
    $fmt.=$noreply;
    my $expiry = shift // 0;
    if($expiry != 0){
        $expiry += time() if $expiry < 315360000;
        if($expiry <= time()){
            # Already expired!
            return $one ne '' ? '' : { map($_ => '', keys %$hash) };
        }
    }

    my %ret = ();
    while(my ($k,$v) = each(%$hash)){
        $ret{$k}=undef;
        unless(defined($v)){
            $@="Cannot store undef for $k";
            carp "$@\n" if $self->{'verbose'};
            next;
        }
        my ($data, $flags) = $self->encode_data($k, $v);
        next unless defined($data);
        my ($sock, $mk) = $self->socket_for_key($k);
        if($sock){
            my $res;
            if ( $cmd eq 'cas' && !defined( $tokens->{$k} ) ) {
                $res = $self->command($sock, sprintf($fmt, 'add', $mk, $flags, $expiry, length($data), '') . "\r\n$data\r\n", !$noreply) // next;
            } else {
                $res = $self->command($sock, sprintf($fmt, $cmd, $mk, $flags, $expiry, length($data), $tokens->{$k} // '') . "\r\n$data\r\n", !$noreply) // next;
            }
            unless($noreply){
                if($res eq 'STORED'){
                    $ret{$k}=1;
                } elsif($res eq 'NOT_STORED' || $res eq 'EXISTS' || $res eq 'NOT_FOUND'){
                    $ret{$k}='';
                } elsif($res =~ s/^ERROR\d*\s+//){
                    $@ = "Memcached $cmd failed: $res";
                    carp "$@\n" if $self->{'verbose'};
                } else {
                    $self->_rm_socket( $sock, "Invalid response to $cmd from %s: $res" );
                }
            }
        }
    }

    return $one ne '' ? $ret{$one} : \%ret;
}

sub set {
    return _set( 'set', @_ );
}

sub add {
    return _set( 'add', @_ );
}

sub replace {
    return _set( 'replace', @_ );
}

sub cas {
    return _set( 'cas', @_ );
}


sub delete {
    my $noreply = defined(wantarray) ? '' : ' noreply' ;
    my ($self, @keys) = @_;

    croak "At least one key must be given" if @keys<1;

    my %ret = ();
    foreach my $k (@keys){
        $ret{$k}=undef;
        my ($sock, $mk) = $self->socket_for_key($k);
        if($sock){
            my $res = $self->command($sock, "delete $mk$noreply\r\n", !$noreply) // next;
            unless($noreply){
                if($res eq 'DELETED'){
                    $ret{$k}=1;
                } elsif($res eq 'NOT_FOUND'){
                    $ret{$k}='';
                } elsif($res =~ s/^ERROR\d*\s+//){
                    $@ = "Memcached delete failed: $res";
                    carp "$@\n" if $self->{'verbose'};
                } else {
                    $self->_rm_socket( $sock, "Invalid response to delete from %s: $res" );
                }
            }
        }
    }

    return @keys==1 ? $ret{$keys[0]} : \%ret;
}

sub touch {
    my $noreply = defined(wantarray) ? '' : ' noreply' ;
    my ($self, $expiry, @keys) = @_;

    if($expiry != 0){
        $expiry += time() if $expiry < 315360000;
        if($expiry <= time()){
            # Pass 1980 to memcached, in case the user passed something stupid
            # like 10-time() that falls in memcached's "30 days" window.
            $expiry = 315360000;
        }
    }

    croak "At least one key must be given" if @keys<1;

    my %ret = ();
    foreach my $k (@keys){
        $ret{$k}=undef;
        my ($sock, $mk) = $self->socket_for_key($k);
        if($sock){
            my $res = $self->command($sock, "touch $mk $expiry$noreply\r\n", !$noreply) // next;
            unless($noreply){
                if($res eq 'TOUCHED'){
                    $ret{$k}=1;
                } elsif($res eq 'NOT_FOUND'){
                    $ret{$k}='';
                } elsif($res =~ s/^ERROR\d*\s+//){
                    $@ = "Memcached touch failed: $res";
                    carp "$@\n" if $self->{'verbose'};
                } else {
                    $self->_rm_socket( $sock, "Invalid response to touch from %s: $res" );
                }
            }
        }
    }

    return @keys==1 ? $ret{$keys[0]} : \%ret;
}


sub _incrdecr {
    my $noreply = defined(wantarray) ? '' : ' noreply' ;
    my ($cmd, $self, $key, $amount) = @_;

    $amount //= 1;
    croak "Invalid amount" if $amount <= 0 || $amount >= 2**64;

    my ($sock, $mk) = $self->socket_for_key($key);
    return undef unless $sock;
    my $res = $self->command($sock, "$cmd $mk $amount$noreply\r\n", !$noreply) // return undef;
    unless($noreply){
        return "0 but true" if $res =~ /^0+\s*$/;
        return +$res if $res =~ /^\d+\s*$/;
        if($res eq 'NOT_FOUND'){
            return '';
        } elsif($res =~ s/^ERROR\d*\s+//){
            $@ = "Memcached $cmd failed: $res";
            carp "$@\n" if $self->{'verbose'};
            return undef;
        } else {
            $self->_rm_socket( $sock, "Invalid response to $cmd from %s: $res" );
            return undef;
        }
    }
}

sub incr {
    return _incrdecr( 'incr', @_ );
}

sub decr {
    return _incrdecr( 'decr', @_ );
}

sub munge_key {
    my $self = shift;
    my $key = shift;
    my $ret = $self->SUPER::munge_key($key);
    $ret = $self->{'namespace'} . $ret if defined( $ret );
    carp "$@\n" if !defined($ret) && $self->{'verbose'};
    return $ret;
}

1;

=pod

=back

=head1 COPYRIGHT

Copyright 2013 Anomie

This library is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

=cut
