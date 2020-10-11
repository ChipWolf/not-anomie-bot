#!/usr/bin/perl -w

use Data::Dumper;

use Cwd;
use File::Basename;
use lib Cwd::realpath( File::Basename::dirname( Cwd::realpath( __FILE__ ) ) . '/../../..' );
use AnomieBOT::API::Cache;

$Data::Dumper::Sortkeys = 1;

die "USAGE: $0 class opt-string\n" unless @ARGV == 2;

my $c = AnomieBOT::API::Cache->create( @ARGV );

sub test {
    my ($name, $expect, $actual) = @_;
    print "$name: ";
    my $e = Dumper( $expect );
    my $a = Dumper( $actual );
    if ( $e eq $a ) {
        print "ok\n";
        return;
    }
    print "FAIL!\n---\n" . Data::Dumper->Dump( [$expect, $actual], ['Expected','Actual'] ) . "---\n";
    exit(1);
}

$c->delete( "foo", "foo2", "foo3", "foo<>bar", "foo<>bar<>baz" );

test('GET unset key', undef, $c->get( "foo" ));

test('Basic SET', 1, $c->set( "foo", "bar" ));
test('Basic GET', "bar", $c->get( "foo" ));

my $obj = { a => 42, b => [ qw/foo bar baz/ ] };
test('Object SET', 1, $c->set( "foo", $obj ));
test('Object GET', $obj, $c->get( "foo" ));

test('Multi-SET', { "foo" => 1, "foo2" => 1 }, $c->set( { "foo" => "bar", "foo2" => $obj } ));
test('Multi-GET', { "foo" => "bar", "foo2" => $obj }, $c->get( qw/foo foo2 foo3/ ));

$c->set( "foo", "bar" );
test('DELETE', 1, $c->delete( "foo" ));
test('GET after DELETE', undef, $c->get( "foo" ));

test('DELETE non-existent', '', $c->delete( "foo" ));
test('DELETE multiple', { foo => '', foo2 => 1 }, $c->delete( "foo", "foo2" ));

test('ADD of non-existent key', 1, $c->add( "foo", "add non-exist" ));
test('ADD of existent key', '', $c->add( "foo", "add exist" ));
test('Correct value after ADDs', "add non-exist", $c->get( "foo" ));

$c->delete( "foo" );
test('REPLACE of non-existent key', '', $c->replace( "foo", "replace non-exist" ));
test('Correct value after REPLACE', undef, $c->get( "foo" ));
$c->set( "foo", "random value" );
test('REPLACE of existent key', 1, $c->replace( "foo", "replace exist" ));
test('Correct value after REPLACE', "replace exist", $c->get( "foo" ));

$c->set( "foo", "random value" );
(undef,$castoken) = $c->gets( "foo" );
test('CAS unchanged', 1, $c->cas( "foo", "cas unchanged", $castoken ));
test('Correct value after CAS', "cas unchanged", $c->get( "foo" ));
test('CAS changed', '', $c->cas( "foo", "cas changed", $castoken ));
test('Correct value after CAS', "cas unchanged", $c->get( "foo" ));
$c->delete( "foo" );
test('CAS deleted', '', $c->cas( "foo", "cas deleted", $castoken ));
test('Correct value after CAS', undef, $c->get( "foo" ));

$c->delete( "foo" );
(undef,$castoken) = $c->gets( "foo" );
test('CAS undefined + unchanged', 1, $c->cas( "foo", "cas unchanged", $castoken ));
test('Correct value after CAS', "cas unchanged", $c->get( "foo" ));
(undef,$castoken) = $c->gets( "foo" );
test('CAS undefined + changed', 1, $c->cas( "foo", "cas unchanged", $castoken ));
test('Correct value after CAS', "cas unchanged", $c->get( "foo" ));

$c->set( "foo", 42 );
test('INCR by 1', '43', $c->incr( "foo" ));
test('INCR by 10', '53', $c->incr( "foo", 10 ));
test('DECR by 1', '52', $c->decr( "foo" ));
test('DECR by 10', '42', $c->decr( "foo", 10 ));

$c->set( "foo", 1 );
test('DECR to 0', "0 but true", $c->decr( "foo" ));

$c->delete( "foo" );
test('INCR nonexistent', '', $c->incr( "foo" ));

$c->set( "foo", "bar" );
test('INCR invalid', undef, $c->incr( "foo" ));

$c->set( "foo<>bar", "random value" );
test('SET with prefix', "random value", $c->get( 'foo<>bar' ));
$c->flush_prefix( 'foo' );
test('Flush prefix worked', undef, $c->get( 'foo<>bar' ));

$c->set( "foo<>bar", "random value" );
$c->set( "foo<>bar<>baz", "random value" );
test('SET with multiple prefixes', { "foo<>bar" => "random value", "foo<>bar<>baz" => "random value" }, $c->get( 'foo<>bar', 'foo<>bar<>baz' ));
$c->flush_prefix( 'foo<>bar' );
test('Flush prefix worked with multiple prefixes', { "foo<>bar" => "random value" }, $c->get( 'foo<>bar', 'foo<>bar<>baz' ));


test('SET with expiry', 1, $c->set( "foo", "bar", 1 ));
test('Multi-SET with expiry', { "foo2" => 1, "foo3" => 1 }, $c->set( { "foo2" => "bar", "foo3" => "bar" }, 1 ));
test('GET before expiry', { "foo" => "bar", "foo2" => "bar" }, $c->get( "foo", "foo2" ));
sleep(3);
test('GET after expiry', {}, $c->get( "foo", "foo2" ));

$c->set( "foo", "random value" );
test('SET with negative expiry', '', $c->set( "foo", "expired?", -1 ));
test('SET with past expiry', '', $c->set( "foo", "expired?", time()-20 ));

$c->set( { "foo" => "no expiry", "foo2" => "updated expiry", "foo3" => "original expiry" }, 1 );
test( 'TOUCH (no expiry)', 1, $c->touch( 0, "foo" ));
test( 'TOUCH (with expiry)', 1, $c->touch( 5, "foo2" ));
sleep(3);
test('GET after original expiry', { "foo" => "no expiry", "foo2" => "updated expiry" }, $c->get( "foo", "foo2", "foo3" ));
sleep(4);
test('GET after updated expiry', { "foo" => "no expiry" }, $c->get( "foo", "foo2", "foo3" ));

test( 'TOUCH (missing key)', '', $c->touch( 0, "foo3" ));
test( 'TOUCH multiple', { foo => 1, foo2 => '' }, $c->touch( 0, "foo", "foo2" ));
