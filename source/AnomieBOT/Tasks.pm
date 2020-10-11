package AnomieBOT::Task;

use utf8;
use strict;

use POSIX qw/strftime/;
use Time::Local qw//;

use vars qw/@ISA @EXPORT_OK %EXPORT_TAGS/;
require Exporter;
@ISA = qw/Exporter/;
@EXPORT_OK = qw/strftime timegm timelocal ISO2timestamp timestamp2ISO onlylist ns2cmtype bunchlist/;
%EXPORT_TAGS = (
    time => [qw/strftime timegm timelocal ISO2timestamp timestamp2ISO/],
);

# Alas, we can't actually use Time::Local because it will not handle dates
# like "2010-01-32". timelocal is the same as mktime, but timegm is rather more
# tricky.
sub timelocal {
    return mktime(@_);
}

use vars qw/$Epoch/;
$Epoch = Time::Local::timegm(0,0,0,1,2,100);
sub timegm {
    my ($sec,$min,$hour,$mday,$mon,$year)=@_;
    my ($r);

    use integer;

    # Start years in March, and make the year relative to 2000
    $mon-=2; $year-=100;

    # Regularize the month and year
    $r=$mon%12; $year += $mon/12 - ($r<0); $mon = $r + 12*($r<0);

    # Convert date into days since March 1 in the year 2000
    my $days=$year*365 + $year/4 - $year/100 + $year/400 + ($mon*306+5)/10 + $mday - 1;
    $days-- if $year%4<0;
    $days++ if $year%100<0;
    $days-- if $year%400<0;

    # Ok, turn into seconds and return
    return $days*86400+$hour*3600+$min*60+$sec+$Epoch;
}

sub ISO2timestamp {
    my $t=shift;
    return undef unless defined($t);
    return timegm($6,$5,$4,$3,$2-1,$1-1900)
        if $t=~/^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)Z$/;
    return undef;
}

sub timestamp2ISO {
    my @t=gmtime(shift);
    return undef unless @t;
    return sprintf('%04d-%02d-%02dT%02d:%02d:%02dZ',
                   $t[5]+1900, $t[4]+1, $t[3], $t[2], $t[1], $t[0]);
}

sub onlylist {
    my ($param, $limit, @list)=@_;
    return (@list <= $limit) ? ($param => join('|',@list)) : ();
}

sub ns2cmtype {
    my @ns=split /\|/, $_[0];
    my @t=();
    push @t, 'subcat' if grep $_ eq 14, @ns;
    push @t, 'file' if grep $_ eq 6, @ns;
    push @t, 'page' if grep !/^(?:6|14)$/, @ns;
    return join('|',@t);
}

sub bunchlist {
    my $count=shift;
    my @ret=();
    push @ret, join('|', splice(@_,0,$count)) while @_;
    return \@ret;
}

=pod

=head1 NAME

AnomieBOT::Task - AnomieBOT task base class

=head1 SYNOPSIS

 use AnomieBOT::Task ();
 use vars qw/@ISA/;
 @ISA = qw/AnomieBot::Task/;

 # Implement methods below

=head1 DESCRIPTION

C<AnomieBOT::Task> is the base class for tasks to be run by AnomieBOT. An
actual task should inherit from this and implement the methods noted below.

=head1 UTILITY METHODS

Various utility methods are available.

=head2 TIME METHODS

These can be imported with the keyword C<:time>.

=over

=item AnomieBOT::Task::timelocal( $sec, $min, $hour, $mday, $mon, $year )

Equivalent to Perl's
L<POSIX::mktime|http://perldoc.perl.org/POSIX.html#mktime>. C<$mon> is
zero-based, and C<$year> is based in 1900.

=item AnomieBOT::Task::timegm( $sec, $min, $hour, $mday, $mon, $year )

Like timelocal, but interprets times in UTC instead of the local timezone.

=item AnomieBOT::Task::strftime( $fmt, $sec, $min, $hour, $mday, $mon, $year )

Equivalent to Perl's
L<POSIX::strftime|http://perldoc.perl.org/POSIX.html#strftime>. C<$mon> is
zero-based, and C<$year> is based in 1900.

=item AnomieBOT::Task::ISO2timestamp( $string )

Converts a timestamp in ISO "yyyy-mm-ddThh:ii:ssZ" format to a timestamp
(seconds since the epoch).

=item AnomieBOT::Task::timestamp2ISO( $ts )

Converts a timestamp to a string in ISO "yyyy-mm-ddThh:ii:ssZ" format.

=back

=head2 OTHER METHODS

=over

=item AnomieBOT::Task::bunchlist( $count, @values )

Groups the items in C<@values> into groups of C<$count> elements, and returns an arrayref.
In other words,

  my $list = bunchlist($count, @values);

is roughly equivalent to

  my $list=[];
  push @$list, join('|', splice(@values, 0, $count)) while @values;

This can be useful to construct the arrayref for an A<AnomieBOT::API::Iterator>.

=item AnomieBOT::Task::onlylist( $key, $limit, @values )

If there are C<$limit> or fewer values in C<@values>, this function returns a
2-element list C<< ($key => join('|',@values)) >>. If there are more than
C<$limit> values, this function returns an empty list.

This is intended for use with C<tltemplates>, C<pltitles>, C<clcategories>, and
the like where the number of values is not known and could exceed the API
limits.

=item AnomieBOT::Task::ns2cmtype( $ns )

Given the value for C<cmnamespace> (a '|'-separated list of namespace numbers),
returns the corresponding value for C<cmtype>.

=back

=head1 METHODS TO IMPLEMENT

=over

=item AnomieBOT::Task->new

Perform any initialization necessary. If you override this, the blessed object
must be a hash with the following properties:

=over

=item order

Mainly useful during bot startup, tasks with lower values will run first.
Default is 0.

=back

Also note that properties with names beginning with C<$> are reserved.

=cut

sub new {
    my $class=shift;
    my $self={};
    ($self->{'taskname'}=$class)=~s/.*:://;
    $self->{'order'}=1;
    bless $self, $class;
    return $self;
}

=pod

=item $task->approved

Return a positive number if the task is officially approved, 0 if it is not, or
negative if it is approved but should not run. This is used to keep the bot
from running unapproved tasks while they are in development, and to divide
tasks among instances of the bot.

The default version returns 0, so once you have an approved task you'll need to
override it.

=cut

sub approved {
    return 0;
}

=pod

=item $task->status

Return a short string indicating the job's status. The default is "ok",
which will often be sufficient.

=cut

sub status {
    return 'ok';
}

=pod

=item $task->run( $api )

Run the task. Ideally, this should not take particularly long to accomplish; if
for example your task is to process a category with 1000 pages, one call to
C<run()> should process a small number (depending on how long each takes) and
then return to allow any other pending tasks to run.

The general structure of this function should be as follows:

=over

=item *

Call C<< $api->task(...) >> to set the name of your task.

=item *

Call C<< $api->read_throttle(...) >> and C<< $api->edit_throttle >> to set the
limits you are approved for.

=item *

Do some work, and return.

=back

The return value is the number of seconds until it is expected that this task
will have more work to do, or undef if the task will never have more work.
Return 0 if more work is immediately available. Note this is only a
recommendation, the task may be called again sooner or later depending on the
behavior of other tasks.

The default version returns undef.

=cut

sub run {
    return undef;
}

1;

=pod

=back

=head1 COPYRIGHT

Copyright 2008–2013 Anomie

This library is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.
