package AnomieBOT::API::Iterator;

use utf8;
use strict;

use Carp;
use AnomieBOT::API;

sub new {
    my ($class, $api, %query) = @_;

    my ($ik,$iv)=(undef,[]);
    while(my ($k,$v)=each(%query)){
        next unless ref($v);
        croak "Invalid query: values must be scalars or arrayrefs\n" unless ref($v) eq 'ARRAY';
        croak "Invalid query: only one arrayref is allowed\n" if defined($ik);
        $ik=$k;
        $iv=[@$v]; # copy array ref so we don't shift the passed parameter
        $query{$ik}=shift(@$iv);
    }

    my $self={
        api     => $api,
        iterkey => $ik,
        iterval => $iv,
        cur_iterval => undef,
        query   => \%query,
        cont    => {},
        res     => [],
    };
    bless $self, $class;
    return $self;
}

sub iterval {
    return $_[0]->{'cur_iterval'};
}

sub cur {
    my $self=shift;
    return undef unless exists($self->{'res'}[0]);
    $self->{'res'}[0]{'_ok_'} = 1;
    return $self->{'res'}[0];
}

sub next {
    my $self=shift;
    my $api=$self->{'api'};

    shift @{$self->{'res'}};
    while(!@{$self->{'res'}}){
        return undef unless exists($self->{'query'});

        $self->{'cur_iterval'}=$self->{'query'}{$self->{'iterkey'}} if defined($self->{'iterkey'});
        my $res=$api->query(%{$self->{'query'}}, %{$self->{'cont'}});
        if($res->{'code'} ne 'success'){
            $res->{'_ok_'} = 0;
            return $res;
        }

        my %res=exists($res->{'query'})?%{$res->{'query'}}:();
        delete $res{'normalized'};
        delete $res{'redirects'};
        delete $res{'interwiki'};
        my @res=values %res;
        if(@res > 1){
            return {
                _ok_  => 0,
                code  => 'notiterable',
                error => 'The result set contained too many nodes under the query node: '.join(', ', keys %res),
            };
        } elsif(@res > 0){
            my $ret=$res[0];
            $ret=[ values %$ret ] if ref($ret) eq 'HASH';
            if(ref($ret) ne 'ARRAY'){
                return {
                    _ok_  => 0,
                    code  => 'wtferror',
                    error => 'The result node list is not an array or hash reference. WTF?',
                };
            }
            $self->{'res'}=$ret;
        }

        if(exists($res->{'query-continue'})){
            my %c=();
            my %p=();
            while(my ($p,$n)=each(%{$res->{'query-continue'}})){
                while(my ($k,$v)=each(%$n)){
                    $c{$k}=$v;
                    $p{$p}=1;
                }
            }
            $self->{'cont'}=\%c;
        } elsif(@{$self->{'iterval'}}){
            $self->{'query'}{$self->{'iterkey'}}=shift(@{$self->{'iterval'}});
            $self->{'cont'}={};
        } else {
            delete $self->{'query'};
            delete $self->{'cont'};
        }
    }

    return $self->cur;
}

1;

=pod

=head1 NAME

AnomieBOT::API::Iterator - AnomieBOT API iterator class

=head1 SYNOPSIS

 use AnomieBOT::API;

 my $api = AnomieBOT::API->new('/path/to/config_file', 1);
 $api->login();
 my $iter = $api->iterator(list=>'allpages', apnamespace=>0, aplimit=>10);
 while(my $res = $iter->next){
     # Do stuff
 }

=head1 DESCRIPTION

C<AnomieBOT::API> is a class implementing various functions needed by a
MediaWiki bot. This class represents an iterator over a result set.

=head1 METHODS

=over

=item $iter->cur

This returns the current result page, or undef if there is no current result.

Note that no API query is done on creation, and thus no result object is
current. C<< $iter->next >> must be called at least once before this function is
useful.

=item $iter->next

This moves the pointer to the next result and returns it, performing API
queries as necessary.

The return object is normally a hashref representing one page object, with an
additional property C<_ok_> set to a true value. If C<_ok_> is false, the
returned hashref is instead the error object as returned by
C<< $api->query() >>. Calling C<< $iter->next >> again after an error will
retry the API query, which may or may not succeed.

When no more results are available, undef is returned.

=back

=head1 COPYRIGHT

Copyright 2008–2013 Anomie

This library is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

=cut
