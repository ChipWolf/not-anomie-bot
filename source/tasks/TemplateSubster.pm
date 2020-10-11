package tasks::TemplateSubster;

=pod

=begin metadata

Bot:      MirahezeBots
Task:     TemplateSubster
Created:  2020-10-11

Subst templates in [[:Category:Templates to be automatically substituted]].
See [[User:MirahezeBots/docs/TemplateSubster|documentation]] for details.

=end metadata

=cut

use utf8;
use strict;

use Data::Dumper;
use AnomieBOT::Task qw/:time/;
use tasks::TemplateSubster::Base;
use vars qw/@ISA/;
@ISA=qw/tasks::TemplateSubster::Base/;

sub new {
    my $class=shift;
    my $self=$class->SUPER::new();
    bless $self, $class;
    return $self;
}

=pod

=for info
ChipWolf 2020-10-11

=cut

sub approved {
    return 3;
}

sub run {
    my ($self, $api)=@_;
    my $res;

    $api->task('TemplateSubster',0,10,qw/d::Redirects d::Templates d::Talk d::IWNS/);

    $res = $self->fetchSig( $api );
    return $res if defined( $res );

    # Spend a max of 5 minutes on this task before restarting
    my $endtime=time()+300;

    # Fetch max transclusions limit
    $res=$api->query(titles=>'User:'.$api->user.'/TemplateSubster max transclusions', prop=>'info|revisions', rvlimit=>1, rvprop=>'content', rvslots=>'main', inprop=>'protection');
    if($res->{'code'} ne 'success'){
        $api->warn("Failed to get max transclusions limit: ".$res->{'error'}."\n");
        return 60;
    }
    $res=(values %{$res->{'query'}{'pages'}})[0];
    my $max_transclusions = 5;
    if(!exists($res->{'missing'})){
        my $ok=0;
        foreach (@{$res->{'protection'}}) {
            $ok=1 if($_->{'type'} eq 'edit' && $_->{'level'} eq 'sysop');
        }
        if(!$ok){
            $api->whine("[[User:".$api->user."/TemplateSubster max transclusions]] is unprotected", "In an effort to prevent disruption, I refuse to subst templates that have over a certain number of transclusions unless they are listed at [[User:".$api->user."/TemplateSubster force]]. But it defeats the purpose if the page defining that number is not fully protected. Please have that page protected.");
            return 300;
        }
        $max_transclusions = $res->{'revisions'}[0]{'slots'}{'main'}{'*'};
        $max_transclusions =~ s/^\s*|\s*$//sg;
        unless($max_transclusions =~ /^\d+$/){
            $api->whine("[[User:".$api->user."/TemplateSubster max transclusions]] is invalid", "The page must contain a single positive integer. No comments, transclusions, or other text. Please fix.");
            return 300;
        }
    }

    $res=$api->query(titles=>'User:'.$api->user.'/TemplateSubster force', prop=>'info|links', pllimit=>'max', inprop=>'protection');
    if($res->{'code'} ne 'success'){
        $api->warn("Failed to get force list: ".$res->{'error'}."\n");
        return 60;
    }
    $res=(values %{$res->{'query'}{'pages'}})[0];
    my %force = ();
    if(!exists($res->{'missing'})){
        my $ok=0;
        foreach (@{$res->{'protection'}}) {
            $ok=1 if($_->{'type'} eq 'edit' && ($_->{'level'} eq 'sysop' || $_->{'level'} eq 'templateeditor'));
        }
        if($ok){
            foreach (@{$res->{'links'}}) {
                $force{$_->{'title'}} = 1;
            }
        } else {
            $api->whine("[[User:".$api->user."/TemplateSubster force]] is unprotected", "In an effort to prevent disruption, I refuse to subst templates that have over $max_transclusions transclusions unless they are listed at [[User:".$api->user."/TemplateSubster force]]. But it defeats the purpose if that page is not fully protected or template-protected. Please have that page protected.");
        }
    }

    # API doesn't have an efficient query, so use the DB
    my ($dbh);
    eval {
        ($dbh) = $api->connectToReplica( 'enwiki' );
    };
    if ( $@ ) {
        $api->warn( "Error connecting to replica: $@\n" );
        return 300;
    }
    my @rows;
    eval {
        @rows = @{ $dbh->selectall_arrayref( qq{
        SELECT page_namespace, page_title, COUNT(distinct tl_from) AS ct
        FROM categorylinks JOIN page ON(cl_from=page_id) LEFT JOIN templatelinks ON(tl_namespace=page_namespace AND tl_title=page_title)
        WHERE cl_to='Wikipedia_templates_to_be_automatically_substituted'
        GROUP BY page_namespace, page_title
        }, { Slice => {} } ) };
    };
    if ( $@ ) {
        $api->warn( "Error fetching page list from replica: $@\n" );
        return 300;
    }
    return 3600 unless @rows;

    my %rns = $api->namespace_reverse_map();
    my %process = ();
    my @tmpl = ();
    for my $row (@rows) {
        utf8::decode( $row->{'page_title'} ); # Data from database is binary
        my $title = $row->{'page_title'};
        $title = $rns{$row->{'page_namespace'}} . ':' . $title if $row->{'page_namespace'} != 0;
        $title =~ s/_/ /g;
        if($row->{'ct'} > $max_transclusions && !($force{$title} // 0)){
            my $iter = $api->iterator(
                list => 'recentchanges',
                rcprop => 'timestamp|ids|user|comment',
                rctype => 'categorize',
                rctitle => 'Category:Wikipedia templates to be automatically substituted',
                rclimit => 'max',
            );
            my $who = '';
            my $who2 = '';
            while ( my $rc = $iter->next ) {
                if ( !$rc->{'_ok_'} ) {
                    $api->warn( "Failed to retrieve recent changes: " . $rc->{'error'} . "\n" );
                    return 60;
                }
                if ( !exists($rc->{'commenthidden'}) && !exists($rc->{'userhidden'}) && $rc->{'comment'} =~ /^\[\[:\Q$title\E\]\] added to category/ ) {
                    $who = ' <small>Possibly added by [[User:' . $rc->{'user'} . ']] at [[Special:Diff/' . $rc->{'revid'} . '|' . $rc->{'timestamp'} . ']].</small>';
                    $who2 = '; possibly added by [[User:' . $rc->{'user'} . ']] in [[Special:Diff/' . $rc->{'revid'} . ']] at ' . $rc->{'timestamp'};
                    last;
                }
            }
            $api->log("Skipping $title, $row->{ct} transclusions > max $max_transclusions" . $who2);
            $api->whine("[[$title]] has too many transclusions", "{{N.b.}} <b>Note that TFD substitutions should now be done via [[User:AnomieBOT/TFDTemplateSubster]] rather than by (ab)using TemplateSubster!</b>\n\nIn an effort to prevent disruption, I refuse to subst templates that have over $max_transclusions transclusions unless they are listed at [[User:".$api->user."/TemplateSubster force]]. Please either edit the template to remove it from [[:Category:Wikipedia templates to be automatically substituted]], manually subst the [{{fullurl:Special:WhatLinksHere/$title|hidelinks=1&hideredirs=1}} existing transclusions], or add it to [[User:".$api->user."/TemplateSubster force]] to let me know it is OK to subst them.$who");
        } else {
            push @tmpl, $title;
            $process{$title} = 0 if $row->{'ct'} > 0;
        }
    }
    return 3600 unless %process; # Nothing to process

    # Don't resolve in case only a redirect should be substed for some strange reason.
    my %r=$api->redirects_to(@tmpl);
    if(exists($r{''})){
        $api->warn("Failed to get redirects: ".$r{''}{'error'}."\n");
        return 60;
    }

    return $self->process( $api, \%process, \%r, $endtime );
}

sub summary {
    my ($self, $api, @remv) = @_;
    my $help="User:".$api->user."/docs/TemplateSubster";

    @remv = map { "{{$_}}" } @remv;
    $remv[-1] = 'and ' . $remv[-1] if @remv > 1;
    return "[[$help|Substing templates]]: " . join( ( @remv > 2 ) ? ', ' : ' ', @remv ) . ". See [[$help]] for info.";
}

1;
