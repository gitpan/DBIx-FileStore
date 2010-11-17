use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'DBIx::FileStore',
    AUTHOR              => q{joshr <joshr>},
    VERSION_FROM        => 'lib/DBIx/FileStore.pm',
    ABSTRACT_FROM       => 'lib/DBIx/FileStore.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
        'Config::File' => 0, 
        'DBI'          => 0,
        'Digest::MD5'  => 0,
        'File::Temp'   => 0,
        'File::Copy'   => 0,
        'List::Util'   => 0,
        'File::Spec::Functions' => 0,

    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'DBIx-FileStore-*' },

    # Added by  joshr
    'EXE_FILES' => [ glob( "script/fdb*" ) ],     # these go to their perl bin dir

);
