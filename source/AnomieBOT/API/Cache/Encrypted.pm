package AnomieBOT::API::Cache::Encrypted;
use parent AnomieBOT::API::Cache;

use utf8;
use strict;

use Data::Dumper;
use Carp;
use Storable;
use Crypt::GCrypt;
use Digest::SHA ();
use IO::Compress::Gzip ();
use IO::Uncompress::Gunzip ();

use constant {
    DATA_ENCRYPTED  => 1<<0,
    DATA_COMPRESSED => 1<<1,
    DATA_FROZEN     => 1<<2,
    DATA_UTF8       => 1<<3,
};

=pod

=head1 NAME

AnomieBOT::API::Cache::Encrypted - AnomieBOT API cache helper for encrypted data

=head1 SYNOPSIS

 use AnomieBOT::API::Cache;

 my $cache = AnomieBOT::API::Cache->create( $class, $optionString );
 $cache->set( 'foo', 'bar' );
 say $cache->get( 'foo' );  # Outputs "bar"

=head1 DESCRIPTION

C<AnomieBOT::API::Cache::Encrypted> adds some utility functions to
A<AnomieBOT::API::Cache> for encrypting the data before storing it.

=head1 METHODS

In addition to the methods inherited from the base class, the following are available.

=over

=item AnomieBOT::API::Cache::Encrypted->new( $optionString )

Creates a new AnomieBOT::API::Cache::Encrypted object, which should be
reblessed as a subclass. The option string is a semicolon-separated list of
key-value pairs; if the value must contain a semicolon or backslash, escape it
using a backslash.

Recognized keys are:

=over

=item encrypt

Key to be used when encrypting the data.

=back

=cut

sub new {
    my ($class, $optionString) = @_;
    my %opts = $class->explode_option_string( $optionString );

    my $self = {
        encrypt => $opts{'encrypt'}//'',
    };

    if($self->{'encrypt'} ne ''){
        # Preprocess key
        utf8::encode( $self->{'encrypt'} ) if utf8::is_utf8( $self->{'encrypt'} );
        $self->{'encrypt'} = Digest::SHA::sha256( $self->{'encrypt'} );
    } else {
        $self->{'encrypt'} = undef;
    }

    bless $self, $class;
    return $self;
}

sub _get_iv {
    my ($self, $key, $blklen) = @_;
    my $iv;

    if(!exists($self->{'ivseed'})){
        # Get IV seed
        $iv = '';
        if(open my $fh, '<:raw', '/dev/urandom'){
            while(length($iv) < 32){
                last if (read($fh, $iv, 32-length($iv), length($iv))//0) <= 0;
            }
            close $fh;
        }
        $iv .= pack("n", rand(2**16)) while length($iv)<32; # Hopefully this isn't needed...
        utf8::encode( $iv ) if utf8::is_utf8( $iv );
        $iv = Digest::SHA::sha256($iv);
        $self->{'ivseed'} = $iv;
    }

    $iv = '';
    while(length($iv) < $blklen){
        my $tmp = pack('n', rand(2**16)) . $self->{'ivseed'} . pack('n', rand(2**16));
        utf8::encode( $tmp ) if utf8::is_utf8( $tmp );
        $self->{'ivseed'} = Digest::SHA::sha256($tmp);
        $tmp = $key;
        utf8::encode( $tmp ) if utf8::is_utf8( $tmp );
        $iv .= Digest::SHA::hmac_sha256( $tmp, $self->{'ivseed'} );
    }
    return substr($iv, 0, $blklen);
}

=pod

=item $cache->encode_data( $key, $value )

Encodes a Perl data value into binary. C<$value> may be a scalar or anything
accepted by L<Storable::freeze()|Storable(3perl)/MEMORY-STORE>.

Returns the binary data and flags on success, or undef on error.

=cut

sub encode_data {
    my ($self, $key, $data) = @_;
    my $flags = 0;

    if(ref($data)){
        eval {
            $data = Storable::nfreeze($data);
        };
        if($@){
            $@ = "Could not store object for $key: $@";
            carp "$@\n" if $self->{'verbose'};
            return undef;
        }
        $flags |= DATA_FROZEN;
    } elsif(utf8::is_utf8($data)){
        utf8::encode($data);
        $flags |= DATA_UTF8;
    }

    if(length($data)>=100){
        my $zdata;
        unless(IO::Compress::Gzip::gzip(\$data => \$zdata, Level => 9, Minimal => 1)){
            $@="Compression failed for $key: $IO::Compress::Gzip::GzipError";
            carp "$@\n" if $self->{'verbose'};
            return undef;
        }
        if(length($zdata)<length($data)){
            $data = $zdata;
            $flags |= DATA_COMPRESSED;
        }
    }

    # Simple integers don't get encrypted (so incr/decr can work)
    if($self->{'encrypt'} && !($flags == 0 && $data=~/^\d+$/ && $data < 2**64)){
        my $cipher = Crypt::GCrypt->new(
            type => 'cipher',
            algorithm => 'aes256',
            mode => 'cbc',
            padding => 'standard',
        );
        $cipher->start('encrypting');
        $cipher->setkey($self->{'encrypt'});
        my $iv = $self->_get_iv($key, $cipher->blklen);
        $cipher->setiv($iv);
        $data = $iv . $cipher->encrypt($data) . $cipher->finish;
        $flags |= DATA_ENCRYPTED;
    }

    return ($data, $flags);
}

=pod

=item $cache->decode_data( $key, $data, $flags )

Decodes binary data into a Perl value, the opposite of
C<< $cache->encode_data() >>.

Returns the Perl value on success, or undef on error.

=cut

sub decode_data {
    my ($self, $key, $data, $flags) = @_;

    if($flags & DATA_ENCRYPTED){
        if(!$self->{'encrypt'}){
            $@="Encryption key not set, cannot decrypt";
            carp "$@\n" if $self->{'verbose'};
            return undef;
        }
        my $cipher = Crypt::GCrypt->new(
            type => 'cipher',
            algorithm => 'aes256',
            mode => 'cbc',
            padding => 'standard',
        );
        if(length($data) % $cipher->blklen){
            $@="Invalid encrypted data for $key";
            carp "$@\n" if $self->{'verbose'};
            return undef;
        }
        $cipher->start('decrypting');
        $cipher->setkey($self->{'encrypt'});
        $cipher->setiv(substr($data,0,$cipher->blklen,''));
        $data = $cipher->decrypt($data) . $cipher->finish;
    }

    if($flags & DATA_COMPRESSED){
        my $zdata;
        unless(IO::Uncompress::Gunzip::gunzip(\$data => \$zdata)){
            $@="Invalid compressed data for $key";
            carp "$@\n" if $self->{'verbose'};
            return undef;
        }
        $data = $zdata;
    }

    if($flags & DATA_FROZEN){
        eval {
            $data = Storable::thaw($data);
        };
        if($@){
            $@="Invalid stored object for $key";
            carp "$@\n" if $self->{'verbose'};
            return undef;
        }
    } elsif($flags & DATA_UTF8){
        unless(utf8::decode($data)){
            $@="Invalid stored UTF-8 string for $key";
            carp "$@\n" if $self->{'verbose'};
            return undef;
        }
    }

    return $data;
}

1;

=pod

=back

=head1 COPYRIGHT

Copyright 2013 Anomie

This library is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

=cut
