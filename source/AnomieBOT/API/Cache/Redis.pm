package AnomieBOT::API::Cache::Redis;
use parent AnomieBOT::API::Cache::Encrypted;

use utf8;
use strict;

use Data::Dumper;
use Carp;
use Redis;
use Digest::SHA ();

=pod

=head1 NAME

AnomieBOT::API::Cache::Redis - AnomieBOT API cache using redis

=head1 SYNOPSIS

 use AnomieBOT::API::Cache;

 my $cache = AnomieBOT::API::Cache->create( 'Redis', $optionString );
 $cache->set( 'foo', 'bar' );
 say $cache->get( 'foo' );  # Outputs "bar"

=head1 DESCRIPTION

C<AnomieBOT::API::Cache::Redis> is an implementation of
A<AnomieBOT::API::Cache> using redis for storage.

=head1 METHODS

In addition to the methods inherited from the base class, the following are available.

=over

=item AnomieBOT::API::Cache::Redis->new( $optionString )

Creates a new AnomieBOT::API::Cache::Redis object. The option string is a
semicolon-separated list of key-value pairs; if the value must contain a
semicolon or backslash, escape it using a backslash.

Recognized keys are:

=over

=item server

Server address, of the form "host:port" for network connections, or
"/path/to/socket" for Unix domain socket connections.

=item namespace

Prefix all keys with this string.

=item noreply

Value should be 0 or 1; the default is 1. When a method is called in a void
context and this is set, a reply will not be waited for.

=item max_size

Maximum size of a data item, after compression. Larger data items will cause
setting functions to return undef. Set 0 to disable. Default is 0.

=item encrypt

Encrypts the data before sending it to memcached, using the specified value as
the encryption key. Default is empty, no encryption.

=item pass

Password to send as an "AUTH" command.

=item verbose

Output errors to stdout.

=back

=cut

sub new {
    my ($class, $optionString) = @_;
    my %opts = $class->explode_option_string( $optionString );

    croak "$class requires a server\n" unless '' ne ($opts{'server'}//'');

    my %info = (
        encoding => undef,
        reconnect => 315576000,
    );
    if($opts{'server'}=~m!/!){
        $info{'sock'} = $opts{'server'};
    } else {
        $info{'server'} = $opts{'server'};
    }
    $info{'password'} = $opts{'pass'} if exists($opts{'pass'});
    my $c = Redis->new( %info );

    my $oldself = $class->SUPER::new($optionString);
    my $self = {
        %$oldself,
        c => $c,
        namespace => $opts{'namespace'}//'',
        noreply => $opts{'noreply'}//1,
        max_size => $opts{'max_size'}//0,
        encrypt => $opts{'encrypt'}//'',
        verbose => $opts{'verbose'}//0,
    };

    bless $self, $class;
    return $self;
}

sub _get {
    my ($tok, $self, @keys) = @_;
    croak "At least one key must be given" if @keys<1;
    my @mk = map { $self->munge_key($_) // '<NA>' } @keys;
    my @values;
    eval { @values = $self->{'c'}->mget( @mk ); };
    if ( $@ ) {
        carp "$@\n" if $self->{'verbose'};
        return undef;
    }
    my %ret = ();
    my @delete = ();
    for ( my $i = 0; $i < @keys; $i++ ) {
        my ($mk, $k, $v) = ($mk[$i], $keys[$i], $values[$i]);
        if ( $mk eq '<NA>' || !defined( $v ) ) {
            $tok->{$k} = undef if $tok;
            #$ret{$k} = undef;
        } elsif ( $v =~ /^\d+$/ ) {
            if ( $tok ) {
                my $tmp = $v;
                utf8::encode( $tmp ) if utf8::is_utf8( $tmp );
                $tok->{$k} = Digest::SHA::sha256( $tmp );
            }
            $ret{$k} = +$v;
        } elsif ( $v =~ /^(\d+)!(.*)$/s ) {
            if ( $tok ) {
                my $tmp = $v;
                utf8::encode( $tmp ) if utf8::is_utf8( $tmp );
                $tok->{$k} = Digest::SHA::sha256( $tmp );
            }
            $ret{$k} = $self->decode_data( $k, $2, $1 );
            push @delete, $mk unless defined( $v );
        } else {
            $tok->{$k} = undef if $tok;
            #$ret{$k} = undef;
            push @delete, $mk;
        }
    }
    eval { $self->{'c'}->del( @delete ); } if @delete;
    my @ret = ();
    if ( @keys == 1 ) {
        push @ret, $ret{$keys[0]};
        push @ret, $tok->{$keys[0]} if $tok;
    } else {
        push @ret, \%ret;
        push @ret, $tok if $tok;
    }
    return @ret;
}

sub get {
    my ($ret) = _get( undef, @_ );
    return $ret;
}

sub gets {
    return _get( {}, @_ );
}


sub _set {
    my $cmd = shift;
    my $self = shift;
    my $hash = shift;
    my $one = '';
    if(!ref($hash)){
        $one = $hash;
        $hash = { $hash => shift };
    }
    my $tokens = {};
    if($cmd eq 'cas' ){
        $tokens = shift;
        croak "When passing a hashref of key-value pairs, you must also pass a hashref of cas tokens" if $one eq '' and !ref($tokens);
        croak "When passing a single key-value pair, you must also pass a single cas token (not a hashref)" if $one ne '' and ref($tokens);
        $tokens = { $one => $tokens } if $one ne '';
    }

    my @opt = ();
    push @opt, 'NX' if $cmd eq 'add';
    push @opt, 'XX' if $cmd eq 'replace';

    my $expiry = shift // 0;
    if($expiry != 0){
        $expiry += time() if $expiry < 315360000;
        if($expiry <= time()){
            # Already expired!
            return $one ne '' ? '' : { map($_ => '', keys %$hash) };
        }
        $expiry -= time();
        push @opt, 'EX', $expiry;
    }

    my $noreply = $self->{'noreply'} && !defined(wantarray) && $cmd ne 'cas';
    push @opt, sub {} if $noreply;

    my %ret = ();
    while(my ($k,$v) = each(%$hash)){
        $ret{$k}=undef;
        unless(defined($v)){
            $@="Cannot store undef for $k";
            carp "$@\n" if $self->{'verbose'};
            next;
        }
        my $mk = $self->munge_key( $k );
        next unless defined($mk);
        my ($data, $flags) = $self->encode_data($k, $v);
        next unless defined($data);
        $data = $flags . '!' . $data if $flags || $data=~/\D/;
        my $res;
        eval {
            if ( $cmd eq 'cas' ) {
                if ( defined( $tokens->{$k} ) ) {
                    $self->{'c'}->watch( $mk );
                    my ($v) = $self->{'c'}->mget( $mk );
                    my $tmp = defined( $v ) ? $v : '';
                    utf8::encode( $tmp ) if utf8::is_utf8( $tmp );
                    if ( defined( $v ) && Digest::SHA::sha256( $tmp ) eq $tokens->{$k} ) {
                        $self->{'c'}->multi;
                        $self->{'c'}->set( $mk, $data, @opt );
                        ($res) = $self->{'c'}->exec;
                    } else {
                        $self->{'c'}->unwatch;
                        $res = undef;
                    }
                } else {
                    $res = $self->{'c'}->set( $mk, $data, 'NX', @opt );
                }
            } else {
                $res = $self->{'c'}->set( $mk, $data, @opt );
            }
        };
        if ( $@ ) {
            carp "$@\n" if $self->{'verbose'};
            next;
        }
        unless($noreply){
            if( ( $res // '' ) eq 'OK' ) {
                $ret{$k}=1;
            } else {
                $ret{$k}='';
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
    my ($self, @keys) = @_;
    my $noreply = $self->{'noreply'} && !defined(wantarray);

    croak "At least one key must be given" if @keys<1;

    my @opt = ();
    push @opt, sub {} if $noreply;

    my %ret = ();
    foreach my $k (@keys){
        $ret{$k}=undef;
        my $mk = $self->munge_key($k);
        if($mk){
            eval {
                my $res = $self->{'c'}->del( $mk, @opt );
                $ret{$k} = $res ? 1 : '' unless $noreply;
            };
            carp "$@\n" if $@ and $self->{'verbose'};
        }
    }

    return @keys==1 ? $ret{$keys[0]} : \%ret;
}

sub touch {
    my ($self, $expiry, @keys) = @_;
    my $noreply = $self->{'noreply'} && !defined(wantarray);

    croak "At least one key must be given" if @keys<1;

    my $cmd = 'persist';
    my @opt = ();
    if($expiry != 0){
        $cmd = 'expire';
        $expiry += time() if $expiry < 315360000;
        if($expiry <= time()){
            # Pass 1980 to memcached, in case the user passed something stupid
            # like 10-time() that falls in memcached's "30 days" window.
            $expiry = 315360000;
        }
        $expiry -= time();
        push @opt, $expiry;
    }
    push @opt, sub {} if $noreply;

    my %ret = ();
    foreach my $k (@keys){
        $ret{$k}=undef;
        my $mk = $self->munge_key($k);
        if($mk){
            eval {
                $ret{$k} = $self->{'c'}->exists($mk) ? 1 : '' unless $noreply;
                $self->{'c'}->$cmd( $mk, @opt );
            };
            carp "$@\n" if $@ and $self->{'verbose'};
        }
    }

    return @keys==1 ? $ret{$keys[0]} : \%ret;
}


sub _incrdecr {
    my ($cmd, $self, $key, $amount) = @_;

    $amount //= 1;
    croak "Invalid amount" if $amount <= 0 || $amount >= 2**64;

    my $mk = $self->munge_key($key);
    return undef unless $mk;

    my ($ret) = eval {
        $self->{'c'}->watch( $mk );
        my ($v) = $self->{'c'}->mget( $mk );
        unless ( defined( $v ) ) {
            $self->{'c'}->unwatch;
            return ('');
        }
        unless ( $v =~ /^\d+$/ ) {
            $self->{'c'}->unwatch;
            die "Redis $cmd failed: value is not a 64-bit unsigned integer";
        }
        $self->{'c'}->multi;
        $cmd .= 'by';
        $self->{'c'}->$cmd( $mk, $amount );
        return $self->{'c'}->exec;
    };
    if ( $@ ) {
        carp "$@\n" if $self->{'verbose'};
        return undef;
    }
    return undef unless defined( $ret );
    return $ret if $ret eq '';
    return $ret ? $ret : "0 but true";
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
    $ret = $self->{'namespace'} . $ret if defined($ret);
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
