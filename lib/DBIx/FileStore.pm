package DBIx::FileStore;
use strict;

# this reads and writes files from the db.  
use DBI;
use Digest::MD5 qw( md5_base64 );
use File::Temp;
use File::Copy;

use DBIx::FileStore::ConfigFile;

use fields qw(  dbh dbuser dbpasswd 
                dbname filetable blockstable  blocksize 
                verbose 
                confhash
                );

our $VERSION = '0.07';  # version also mentioned in POD below.

sub new {
    my ($self) = @_;
    unless (ref $self) {
        $self = fields::new($self);
        #$self->{_Foo_private} = "this is Foo's secret";
    }

    my $config_reader = new DBIx::FileStore::ConfigFile();
    my $conf = $self->{confhash} = $config_reader->read_config_file();
    
    my $block_size = 512 * 1024;        # 512K blocks
    #   with 900K blocks, we get: 
    #   DBD::mysql::db do failed: Got a packet bigger than 
    #   'max_allowed_packet' bytes at lib/DBIx/FileStore.pm line 165.
    $self->{blocksize}   = $block_size;

    $self->{dbuser}      = $conf->{dbuser} || die "$0: no dbuser set\n";
    $self->{dbpasswd}    = $conf->{dbpasswd} || die "$0: no dbpasswd set\n";
    $self->{dbname}      = $conf->{dbname} || die "$0: no dbname set\n";

    $self->{filetable}   = "files";
    $self->{blockstable} = "fileblocks";

    my $local_dsn = "DBI:mysql:database=$self->{dbname};host=127.0.0.1";
    my %attr  = ( RaiseError => 1, PrintError => 1, AutoCommit => 1 );  # for mysql

    $self->{dbh} = DBI->connect_cached( 
        $local_dsn, $self->{dbuser}, $self->{dbpasswd}, \%attr);  
    $self->{dbh}->{mysql_auto_reconnect} = 1;   # auto reconnect

    return $self;
}


# reads the content into $pathname, returns the length of the data read.
sub read_from_db {
    my ($self, $pathname, $fdbname) = @_;

    die "$0: name not ok: $fdbname" unless name_ok($fdbname);

    my $fh = $self->{fh};

    # this is a function used as a callback and called with each chunk of the data 
    # into a temporary file
    my $callback = sub {    
        # this is a closure, so $fh comes from the surrounding context
        my $content = shift;
        unless($fh) {
            my ($tfh, $filename) = File::Temp::tempfile( "FileDB-tmp-XXXXX", 
                { DIR => "/tmp", UNLINK => 1 } );
            $fh = $tfh;
        }
        print $fh $content;
    };


    # read all our blocks, calling our callback for each one
    my $ret = $self->_read_blocks_from_db( \&print_to_file_callback, $fdbname ); 
        
    # that's our filehandle!
    $self->{fh} = $fh;

    # if we fetched *something* into our scoped $fh to the temp file,
    # then copy it to the destination they asked for, and delete
    # the temp file.
    if ($fh) {  
        seek($fh, 0, 0); # rewind tmp file
        File::Copy::copy($fh, $pathname) || die "$0: Copy from tmpfile to $pathname failed: $!";
        close($fh) if defined($fh);  # this 'close' should cause the associated file to be deleted
    } else {
        warn "$0: not found in FileDB: $fdbname\n";
    }

    # clear our fh member
    $self->{fh} = $fh = undef;

    # return number of bytes read.
    return $ret;    
}

# returns the length of the data read,
# calls &$callback( $block ) for each block read.
sub _read_blocks_from_db {
    my ($self, $callback, $fdbname) = @_;
        # callback is called on each block, like &$callback( $block )
    my $dbh = $self->{dbh};
    my $verbose = $self->{verbose};
    my $filetable = $self->{filetable};
    my $cmd = "select name, b_md5, c_md5, b_num, c_len from $filetable where name like ? order by b_num";
    my @params = ( $fdbname . ' %' );
    #print "CMD = $cmd -- @params\n";
    warn "$0: Fetching rows $fdbname" if $verbose;
    $dbh->do("lock tables $self->{filetable} read, $self->{blockstable} read");
    my $sth = $dbh->prepare($cmd);
    my $rv =  $sth->execute( @params );
    my $rownum = 0;
    my $c_len  = 0;
    my $orig_c_md5;
    while( my $row = $sth->fetchrow_arrayref() ) {
        $c_len  ||= $row->[5];
        unless ($row && defined($row->[0])) {
            warn "$0: Error: bad row returned?";
            next;
        }
        print "$row->[0]\n" if $verbose;
        unless ($row->[0] =~ /\s+(\d+)$/) {
            warn "$0: Skipping block that doesn't match ' [0-9]+\$': $row->[0]";
            next;
        }
        my $name_num = $1;
        $orig_c_md5 ||= $row->[2];

        # check the MD5...
        if ($row->[2] ne $orig_c_md5) { die "$0: Error: Is DB being updated? Bad content md5sum for $row->[0]\n"; }
        die "$0: Error: our count is row num $rownum, but file says $name_num" 
            unless $rownum == $name_num;
        
        my ($block) =  $dbh->selectrow_array("select block from fileblocks where name=?", {}, $row->[0]);
        die "$0: Bad MD5 checksum for $row->[0] ($row->[1] != " . md5_base64( $block ) 
            unless ($row->[1] eq md5_base64( $block ));

        &$callback( $block );   # call the callback, and pass it the block!

        $rownum++;
    }
    $dbh->do("unlock tables");
    return $c_len;  # for your inspection
}


# my $bytes_written = $self->write_to_db( $localpathname, $filestorename );
sub write_to_db {
    my ($self, $pathname, $fdbname) = @_;

    die "$0: name not ok: $fdbname" unless name_ok($fdbname);

    open(my $fh, "<" , $pathname) 
        || die "$0: Couldn't open: $pathname\n";
    
    my $verbose = $self->{verbose};
    print "Computing md5 of $pathname...\n" if $verbose;
    my $ctx = Digest::MD5->new; 
    $ctx->addfile( $fh );
    my $c_md5 = $ctx->b64digest;    # this reads the file apparently
    #print "MD5 ($_) = $c_md5\n";

    # get the length and rewind the file
    my $total_length = -s $pathname;   
    seek($fh, 0, 0);            # rewind file 

    if ($total_length == 0) { warn "$0: warning: writing 0 bytes for $pathname\n"; }

    my $dbh = $self->{dbh};
    my $filetable = $self->{filetable};
    my $blockstable = $self->{blockstable};
    $dbh->do("lock table $filetable write, $blockstable write");
    for( my ($bytes,$part,$block) = (0,0,"");           # init
    $bytes = read($fh, $block, $self->{blocksize});    # test 
    $part++, $block="" ) {                              # increment
        #warn "Read $bytes bytes of $pathname...\n";
        my $b_md5 = md5_base64( $block );

        printf("saving from %s into '%s '", $pathname, $fdbname, $part) 
            if($verbose && $part==0);

        print "$part." if ($verbose && $part % 25 == 0);

        my $name = sprintf("%s %05d", $fdbname, $part);
        $dbh->do("replace into $filetable set name=?, c_md5=?, b_md5=?, c_len=?, b_num=?", {}, 
            $name, $c_md5, $b_md5, $total_length, $part);
        $dbh->do("replace into $blockstable set name=?, block=?", {}, 
            $name, $block);
        #warn "Wrote $bytes bytes to block $pathname...\n";
    }
    $dbh->do("unlock tables");
    close ($fh) || die "$0: couldn't close: $pathname\n";
    print "\n" if $verbose;
    return $total_length;
}

# warns and returns 0 if passed filename ending with like '/tmp/file 1'
# else returns 1, ie, that the name is OK
# Note that this is a FUNCTION, not a METHOD
sub name_ok {
    my $file = shift;
    if ($file && $file =~ /\s/) {
        warn "$0: Can't use filedbname containing spaces\n";
        return 0;
    }
    if (length($file) > 75) {
        warn "$0: Can't use filedbname longer than 75 chars\n";
        return 0;
    }
    return 1;
}

1;

=pod

=head1 NAME

DBIx::FileStore - Module to store files in a DBI backend

=head1 VERSION

Version 0.07

=head1 SYNOPSIS

Ever wanted to store files in a database? Yeah, it's 
probably a bad idea, but maybe you want to do it anyway. 

This code helps you do that.

Internally all the fdb tools in script/ use this library to 
get at file names and contents in the database.

To get started, see the files QUICKSTART.txt and README
from the DBIx-FileStore distribution.  This document details 
the module's implementation.

=head1 FILENAME NOTES

The name of the file in the filestore cannot contain spaces.

The maximum length of the name of a file in the filestore
is 75 characters.

You can store files under any name you wish in the filestore. 
The name need not correspond to the original name on the filesystem.

All filenames in the filestore are in one flat address space.
You can use / in filenames, but it does not represent an actual
directory. (Although fdbls has some support for viewing files in the 
filestore as if they were in folders. See the docs on 'fdbls' for details.)

=head1 IMPLEMENTATION CAVEAT

NOTE THAT THIS IS A PROOF-OF-CONCEPT DEMO.

THIS WAS NOT DESIGNED AS PRODUCTION CODE.

In particular, we wouldn't have one row in the 'files' table for
each block in the 'fileblocks' table, we'd have one row per file.

Also, we'd probably use a unique ID to address the blocks in the
fileblocks table, instead of the 'name' field that's currently used.

That having been said, this example works quite nicely, and altering
the DB Schema and code would not be a large effort.

=head1 IMPLEMENTATION

The data is stored in the database using two tables: 'files' and 
'fileblocks'.  All meta-data is stored in the 'files' table, 
and the file contents are stored in the 'fileblocks' table.

=head2 fileblocks table

The fileblocks table has only three fields:

=head3 name 

The name of the block. Always looks like "filename.txt <BLOCKNUMBER>",
for example "filestorename.txt 00000".

=head3 block

The contents of the named block. Each block is currently to be 512K.
Care must be taken to use blocks that are not larger than
mysql buffers can handle (in particular, max_allowed_packet).

=head3 timestamp

The timestamp of when this block was inserted into the DB or updated.

=head2 files table

The files table has several fields. There is one row in the files table 
for each row in the fileblocks table-- not one per file (see IMPLEMENTATION 
CAVEATS, above). The fields in the files table are:

=head3 name 

The name of the block, exactly as used in the fileblocks table. 
Always looks like "filename.txt <BLOCKNUMBER>",
for example "filestorename.txt 00000".

=head3 c_len 

The content length of the whole file (sum of length of all the file's blocks).

=head3 b_num

The number of the block this row represents. The b_num is repeated as a five
digit number at the end of the name field (see above). We denormalized
the data like this so you can quickly find blocks by name or block number.

=head3 b_md5

The md5 checksum for the block (b is for 'block') represented by this row.

=head3 c_md5

The md5 checksum for the whole file (c is for 'content') represented by this row.

=head3 lasttime

The timestamp of when this row was inserted into the DB or updated.

=head2 See the file 'table-definitions.sql' for more details about 
the db schema used.

=head1 METHODS

=head2 my $filestore = new DBIx::FileStore()

returns a new DBIx::FileStore object

=head2 my $bytecount = $filestore->read_from_db( "filesystemname.txt", "filestorename.txt" );

Copies the file 'filestorename.txt' from the filestore to the file filesystemname.txt
on the local filesystem.

=head2 my $bytecount = $filestore->_read_blocks_from_db( $callback_function, $fdbname );

** Intended for internal use by this module. ** 

Fetches the blocks from the database for the file stored under $fdbname,
and calls the $callback_function on each one after it is read.

Locks the relevant tables while data is extracted. Locking should probably 
be configurable by the caller.

It also confirms that the MD5 checksum for each block and the file contents
as a whole are correct. Die()'s with an error if a checksum doesn't match.

=head2 my $bytecount = $self->write_to_db( $localpathname, $filestorename );

Copies the file $localpathname from the filesystem to the name
$filestorename in the filestore.

Locks the relevant tables while data is extracted. Locking should probably 
be configurable by the caller.

Returns the number of bytes written. Dies with a message if the source
file could not be read. 

Note that it currently reads the file twice: once to compute the MD5 checksum
before insterting it, and a second time to insert the blocks.

=head1 FUNCTIONS

=head2 my $filename_ok = DBIx::FileStore::name_ok( $fdbname )

Checks that the name $fdbname is acceptable for using as a name
in the filestore. Must not contain spaces or be over 75 chars.

=head1 AUTHOR

Josh Rabinowitz, C<< <Josh Rabinowitz> >>

=head1 SUPPORT

You should probably read the documentation for the various filestore command-line
tools:

  fdbcat, fdbcp, fdbget, fdbls, fdbmv, fdbput, fdbrm, fdbstat, and fdbtidy.
  fdbslurp (which is the reverse of fdbcat) was not completed.


You can also look for information at:

=over 4

=item * Search CPAN

L<http://search.cpan.org/dist/DBIx-FileStore/>

=back

=head1 LICENSE AND COPYRIGHT

Copyright 2010 Josh Rabinowitz.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1; # End of DBIx::FileStore
