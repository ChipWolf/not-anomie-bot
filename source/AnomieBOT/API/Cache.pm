package AnomieBOT::API::Cache;

use utf8;
use strict;

use Digest::SHA ();

=pod

=head1 NAME

AnomieBOT::API::Cache - AnomieBOT API cache base class

=head1 SYNOPSIS

 use AnomieBOT::API::Cache;

 my $cache = AnomieBOT::API::Cache->create( $subclass, $optionString );
 $cache->set( 'foo', 'bar' );
 say $cache->get( 'foo' );  # Outputs "bar"

=head1 DESCRIPTION

C<AnomieBOT::API::Cache> is a base class for non-persistent caching of data.
The interface is generally inspired by memcached.

=head1 KEYS

The general idea is that data items can be stored under "keys", which are ASCII
strings, no whitespace or angle brackets, with a maximum length of 250 bytes.

To make things easier to use, the following 'magic' sequences are recognized in a key:

=over

=item <>

Marks a "prefix" in the key, which may be flushed with
C<< $cache->flush_prefix() >>. Note that keys ending with C<< <> >> are
reserved for internal use.

=item <<I<text>>>

Internally, I<text> is replaced with a hash of the text. This is useful if a
page title is to be included in the key to avoid exceeding the 250-byte limit.

=back

Note that the magic sequences may internally expand to more bytes than they use
in the original string, causing excess-key-length errors.

=head1 METHODS

=over

=item AnomieBOT::API::Cache->create( $subclass, $optionString )

Creates a new AnomieBOT::API::Cache object of the specified subclass, passing
it the specified option string. The subclass will be
"AnomieBOT::API::Cache::$subclass"; see the documentation for each subclass for
details on the options string.

=cut

sub create {
    my ($class, $subclass, $optionString) = @_;
    my $file;

    $class = $class . '::' . $subclass;
    ($file = "$class.pm")=~s!::!/!g;
    require $file;
    return $class->new( $optionString );
}

=pod

=item $cache->get( $key, ... )

If only one key is passed, returns the value stored for C<$key>, or undef if
the key does not exist.

If multiple keys are passed, returns a hash mapping each key to the stored
value. If the key does not exist in the cache, it will not be present in the
returned hash.

=cut

sub get;

=pod

=item $cache->gets( $key, ... )

If only one key is passed, returns a list of two values: the value (if any)
stored for C<$key> and a token for use with C<cas()>.

If multiple keys are passed, returns two hashes: one mapping each key to the
stored value and a second mapping each key to the CAS token. If the key does
not exist in the cache, it will not be present in the first hash.

Note that tokens may be undef; this is not an error.

=cut

sub gets;

=pod

=item $cache->set( $key, $value )

=item $cache->set( $key, $value, $expiry )

=item $cache->set( \%hash )

=item $cache->set( \%hash, $expiry )

Stores one or more values to the cache. The forms including a hashref are
equivalent to multiple calls to C<< $cache->set() >>.

Values may be anything accepted by L<Storable::freeze|Storable(3perl)/MEMORY-STORE>.

If specified, C<$expiry> specifies when the cache item should expire. The value
is considered the number of seconds the value is valid for if less than or
equal to 315360000 (approximately 10 years), or a Unix epoch value (number of
seconds since 1970-01-01T00:00:00Z) if greater.

The first two forms return a boolean (true on success, false on failure), or
undef on error; the forms taking a hashref return a hash mapping each key to a
boolean or undef.

=cut

sub set;

=pod

=item $cache->add( $key, $value )

=item $cache->add( $key, $value, $expiry )

=item $cache->add( \%hash )

=item $cache->add( \%hash, $expiry )

Just like C<< $cache->set() >>, except that the value is only stored if the key
doesn't already exist in the cache.

=cut

sub add;

=pod

=item $cache->replace( $key, $value )

=item $cache->replace( $key, $value, $expiry )

=item $cache->replace( \%hash )

=item $cache->replace( \%hash, $expiry )

Just like C<< $cache->set() >>, except that the value is only stored if the key
already exists in the cache.

=cut

sub replace;

=pod

=item $cache->cas( $key, $value, $token )

=item $cache->cas( $key, $value, $token, $expiry )

=item $cache->cas( \%hash, \%tokens )

=item $cache->cas( \%hash, \%tokens, $expiry )

Just like C<< $cache->set() >>, except that the value is only stored if the
value for the key has not been modified since the CAS token was fetched from
C<< $cache->gets() >>.

Note that, in the hashref case, C<\%hash> holds the key-value pairs to be
stored while C<\%tokens> holds the tokens for each key.

=cut

sub cas;

=pod

=item $cache->delete( $key, ... )

Deletes the specified keys from the cache. Return values are the same as for
C<< $cache->set() >>.

=cut

sub delete;

=pod

=item $cache->flush_prefix( $prefix )

Causes all keys beginning with C<$prefix> followed by C<< <> >> to be effectively
removed from the cache. Note this may not actually delete the keys, it may just
make them inaccessible by changing how C<< <> >> is munged.

=cut

sub flush_prefix {
    my ($self,$prefix) = @_;

    if(!$self->incr("$prefix<>")){
        $self->set("$prefix<>", time()-946684800);
    }
}

=pod

=item $cache->incr( $key )

=item $cache->incr( $key, $amount )

=item $cache->decr( $key )

=item $cache->decr( $key, $amount )

Increment or decrement the value of the key. Note that the key must already
exist in the cache, and the value and amount must be positive integers.

C<$amount> defaults to 1 if not given. Returns the new value for the key on
success ("0 but true" if the value is 0), false on failure, or undef on error.

=cut

sub incr;

sub decr;

=pod

=item $cache->touch( $expiry, $key, ... )

Updates the expiry of the specified keys from the cache. Return values are the
same as for C<< $cache->set() >>.

=cut

sub touch;

=pod

=back

=head1 INTERNAL METHODS

These methods are intended for use by subclasses.

=over

=item AnomieBOT::API::Cache->explode_option_string( $optionString )

Converts an option string of the form "key=value;key=value" to a hash.
Semicolons and backslashes must be escaped with a backslash.

=cut

sub explode_option_string {
    my ($self, $opts) = @_;

    my %ret = ();
    while($opts=~/\G((?:[^\\;]++|\\.)*)(?:;|$)/g){
        my $pair=$1;
        $pair=~s/^\s+|\s+$//g;
        next if $pair eq '';
        die "Invalid AnomieBOT::API::Cache option string\n" unless $pair=~/^(.+?)\s*=\s*(.*?)$/;
        die "Duplicate key '$1' in AnomieBOT::API::Cache option string\n" if exists($ret{$1});
        $ret{$1}=$2;
    }
    return %ret;
}

=pod

=item $cache->munge_key( $key )

Replaces the 'magic' tokens in C<$key> and validates it. Returns the munged key
on success, or undef on failure. May call C<< $cache->munge_prefix() >> and/or
C<< $cache->munge_hash() >>.

=item $cache->munge_prefix( $prefix )

Used to handle the C<< <> >> magic token in a key. C<$prefix> is the bit of the
key before the C<< <> >>. Returns some bit of text to insert inside the C<< <> >>.
Do not include angle brackets, whitespace, or non-ASCII characters in your
return value, it will break things.

The default implementation tries to look up the key C<< "$prefix<>" >>. If not
found, a new random value is stored under that key. In either case, the value
is returned.

If you change C<< $cache->flush_prefix() >>, you'll probably want to change
this too. If nothing else, have it return the empty string.

=item $cache->munge_hash( $text )

Used to handle the C<<< <<I<text>>> >>> magic token in a key. Returns the hash
text, by default the base-64 representation of SHA-256.

=cut

sub munge_key {
    my ($self, $key) = @_;

    my $k = $key;

    # We check these beforehand because we use them as temporary replacements
    # for <> below.
    if($k=~/[\x02\x03]/){
        $@="Key '$key' contains invalid characters";
        return undef;
    }

    # Replace all hashes
    $k=~s/<<(.+?)>>/ "\x02".$self->munge_hash($1)."\x03" /ge;

    # Protect prefix-tag at the end of the string
    $k=~s/<>$/\x02\x03/;

    # Replace all other prefix-tags, end-to-start
    while($k=~s/^(.*)<>/ $1."\x02".$self->munge_prefix($1)."\x03" /e){}

    # Check for invalid characters. We already checked for \x02 and \x03 above,
    # so any < or > left is invalid.
    if($k=~/[^\x02\x03\x21-\x3b\x3d\x3f-\x7e]/){
        $@="Key '$key' contains invalid characters";
        return undef;
    }

    # Check length
    if(length($k)>250){
        $@="Key '$key' is too long (" . length($k) . " bytes)";
        return undef;
    }

    # Now we can replace those \x02 and \x03 with valid ASCII.
    $k=~y/\x02\x03/<>/;
    utf8::downgrade($k);
    return $k;
}

sub munge_prefix {
    my ($self, $prefix) = @_;

    my $idx = $self->get("$prefix<>");
    unless($idx && $idx=~/^\d+$/){
        $idx=time()-946684800;
        $self->set("$prefix<>", $idx);
    }
    return $idx;
}

sub munge_hash {
    my ($self, $text) = @_;
    utf8::encode( $text ) if utf8::is_utf8( $text );
    return Digest::SHA::sha256_base64($text);
}

1;

=pod

=back

=head1 COPYRIGHT

Copyright 2013 Anomie

This library is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

=cut
