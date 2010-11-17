#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'DBIx::FileStore' ) || print "Bail out!
";
}

diag( "Testing DBIx::FileStore $DBIx::FileStore::VERSION, Perl $], $^X" );
