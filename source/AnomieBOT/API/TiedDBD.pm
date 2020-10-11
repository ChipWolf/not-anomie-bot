package AnomieBOT::API::TiedDBD;

use utf8;
use strict;

use Data::Dumper;
use JSON;
use Carp;
use DBI;

my $json = JSON->new->allow_nonref->utf8->shrink;

sub TIEHASH {
    my $class=shift;
    my ($dbh, $table, $keycol, $valcol, %wherefields) = @_;

    croak "DBI::db instance required" unless $dbh->isa('DBI::db');

    my $self={
        dbh => $dbh,
        error_handler => \&generic_handle_error,
    };
    bless $self, $class;

    $self->{'error_handler'}=\&sqlite_handle_error if $dbh->get_info(17) eq 'SQLite';
    $self->{'error_handler'}=\&mysql_handle_error if $dbh->get_info(17) eq 'MySQL';

    my $tries = 0;
    RETRY: {
        eval {
            $dbh->selectrow_array("SELECT 1 FROM $table LIMIT 1");

            my $where="WHERE ".join(' AND ', map("$_ = ".$dbh->quote($wherefields{$_}), keys(%wherefields)));
            my $wherekey=$where . (%wherefields ? " AND " : "") . "$keycol = ?";
            $self->{'select'}=$dbh->prepare("SELECT $valcol FROM $table $wherekey") || croak("Could not prepare select: ", $dbh->errstr);
            my $q="INSERT INTO $table (".join(',', keys(%wherefields), $keycol, $valcol).") VALUES (".join(',', map($dbh->quote($_), values(%wherefields)), '?', '?').')';
            $self->{'insert'}=$dbh->prepare($q) || croak("Could not prepare insert: ", $dbh->errstr);
            $self->{'update'}=$dbh->prepare("UPDATE $table SET $valcol=? $wherekey") || croak("Could not prepare update: ", $dbh->errstr);
            $self->{'delete'}=$dbh->prepare("DELETE FROM $table $wherekey") || croak("Could not prepare delete: ", $dbh->errstr);
            $self->{'clear'}=$dbh->prepare("DELETE FROM $table $where") || croak("Could not prepare clear: ", $dbh->errstr);
            $self->{'fetchkeys query'}="SELECT $keycol, $valcol FROM $table $where";
        };
        if($@){
            redo if $tries++ < 60 && $self->{'error_handler'}($self->{'dbh'}, $tries);
            confess $@;
        }
    }

    return $self;
}

sub generic_handle_error {
    return 0;
}

sub sqlite_handle_error {
    my $dbh = shift;
    return 0 unless $dbh->err == 5;
    carp "Database locked, retry in 5 seconds...";
    sleep 5;
    return 1;
}

sub mysql_handle_error {
    my $dbh = shift;
    my $tries = shift;
    if($dbh->err == 2006 || $dbh->err == 2013) {
        carp "Caught MySQL error " . $dbh->err . " (" . $dbh->errstr . "), retrying in ${tries}s";
        sleep $tries; # Linear backoff
        return 1;
    }
    carp "MySQL error! err=" . $dbh->err . "  errstr=" . $dbh->errstr;
    return 0;
}

sub FETCH {
    my $self=shift;
    my $key=shift;
    my $value;

    my $tries = 0;
    RETRY: {
        eval {
            $self->{'select'}->execute($key) || croak("FETCH: select execution failed: ", $self->{'dbh'}->errstr);
            $value = $self->{'select'}->fetch;
            $self->{'select'}->finish;
        };
        if($@){
            redo if $tries++ < 60 && $self->{'error_handler'}($self->{'dbh'}, $tries);
            confess $@;
        }
    }
    $value=$value->[0] if defined($value);
    return defined($value)?$json->decode($value):undef;
}

sub STORE {
    my $self=shift;
    my $key=shift;
    my $value=shift;

    my $tries = 0;
    RETRY: {
        eval {
            if($self->EXISTS($key)){
                $self->{'update'}->execute($json->encode($value), $key) || croak("STORE: update execution failed: ", $self->{'dbh'}->errstr);
            } else {
                $self->{'insert'}->execute($key, $json->encode($value)) || croak("FETCH: insert execution failed: ", $self->{'dbh'}->errstr);
            }
        };
        if($@){
            redo if $tries++ < 60 && $self->{'error_handler'}($self->{'dbh'}, $tries);
            confess $@;
        }
    }
}

sub DELETE {
    my $self=shift;
    my $key=shift;

    my $tries = 0;
    RETRY: {
        eval {
            $self->{'delete'}->execute($key) || croak("DELETE: delete execution failed: ", $self->{'dbh'}->errstr);
        };
        if($@){
            redo if $tries++ < 60 && $self->{'error_handler'}($self->{'dbh'}, $tries);
            confess $@;
        }
    }
}

sub CLEAR {
    my $self=shift;

    my $tries = 0;
    RETRY: {
        eval {
            $self->{'clear'}->execute() || croak("CLEAR: clear execution failed: ", $self->{'dbh'}->errstr);
        };
        if($@){
            redo if $tries++ < 60 && $self->{'error_handler'}($self->{'dbh'}, $tries);
            confess $@;
        }
    }
}

sub EXISTS {
    my $self=shift;
    my $key=shift;
    my $ret;

    my $tries = 0;
    RETRY: {
        eval {
            $self->{'select'}->execute($key) || croak("EXISTS: select execution failed: ", $self->{'dbh'}->errstr);
            $ret = $self->{'select'}->fetch;
            $self->{'select'}->finish;
        };
        if($@){
            redo if $tries++ < 60 && $self->{'error_handler'}($self->{'dbh'}, $tries);
            confess $@;
        }
    }
    return defined($ret);
}

sub FIRSTKEY {
    my $self=shift;

    my $tries = 0;
    RETRY: {
        eval {
            if(exists($self->{'fetchkeys'})){
                $self->{'fetchkeys'}->finish();
                delete $self->{'fetchkeys'};
            }
            $self->{'fetchkeys'}=$self->{'dbh'}->prepare($self->{'fetchkeys query'}) || croak("Could not prepare FIRSTKEY query: ", $self->{'dbh'}->errstr);
            $self->{'fetchkeys'}->execute() || croak("FIRSTKEY: fetchkeys execution failed: ", $self->{'dbh'}->errstr);
        };
        if($@){
            redo if $tries++ < 60 && $self->{'error_handler'}($self->{'dbh'}, $tries);
            confess $@;
        }
    }
    return $self->NEXTKEY;
}

sub NEXTKEY {
    my $self=shift;
    return wantarray ? () : undef unless exists($self->{'fetchkeys'});

    my $r;
    my $tries = 0;
    RETRY: {
        eval {
            $r = $self->{'fetchkeys'}->fetch;
            unless(defined($r)){
                $self->{'fetchkeys'}->finish();
                delete $self->{'fetchkeys'};
                return undef;
            }
        };
        if($@){
            redo if $tries++ < 60 && $self->{'error_handler'}($self->{'dbh'}, $tries);
            confess $@;
        }
    }
    return wantarray ? ($r->[0], defined($r->[1])?$json->decode($r->[1]):undef) : $r->[0];
}

sub UNTIE {
    my $self=shift;
    $self->DESTROY;
}

sub DESTROY {
    my $self=shift;
    delete $self->{'dbh'};
    delete $self->{'select'};
    delete $self->{'insert'};
    delete $self->{'update'};
    delete $self->{'delete'};
    delete $self->{'clear'};
    if(exists($self->{'fetchkeys'})){
        eval { $self->{'fetchkeys'}->finish(); };
        delete $self->{'fetchkeys'};
    }
}

1;
