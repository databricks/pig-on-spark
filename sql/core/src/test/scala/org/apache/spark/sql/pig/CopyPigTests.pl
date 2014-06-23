use 5.012;

##############################################################################
#  Sub: readCfg
# Read the configuration file.  The config file is in Perl format so we'll
# just eval it.  If anything goes wrong we'll complain and quit.
#
# Shamelessly copied from pig/test/e2e/harness/test_harness.pl
#
# Var: cfgFile
# Full path name of config file
#
# Returns:
# returns reference to hash built from cfg file.
#
sub readCfg($)
{
	my $cfgFile = shift;

	open CFG, "< $cfgFile" or die "FATAL ERROR $0 at ".__LINE__.":  Can't open $cfgFile, $!\n";

	my $cfgContents;

	$cfgContents .= $_ while (<CFG>);

	close CFG;

	my $cfg = undef;
        eval("$cfgContents");
	#my $cfg = eval("$cfgContents");

	if ($@) {
		chomp $@;
		die "FATAL ERROR $0 at ".__LINE__." : Error reading config file <$cfgFile>, <$@>\n";
	}

	if (not defined $cfg) {
		die "FATAL ERROR $0 at ".__LINE__." : Configuration file <$cfgFile> should have defined \$cfg\n";
	}

	# Add the name of the file
	$cfg->{'file'} = $cfgFile;

	return $cfg;
}

my $src_file = '/Users/Greg/Pig/pig/test/e2e/pig/tests/nightly.conf';
my $cfg = readCfg($src_file);

my $test_dir = '/Users/Greg/Spark/spark/sql/core/src/test/scala/org/apache/spark/sql/pig/tests/';

my @groups = $cfg->{'groups'};

while (my ($index, $elem) = each @groups[0]) {
    my $name = $elem->{'name'};
    my @tests = $elem->{'tests'};
    while (my ($test_index, $test) = each @tests[0]) {
        my $pig = $test->{'pig'};
        my $dst_file = "$test_dir$name-$test_index";
        open(my $fh, '>', $dst_file);
        print $fh $pig;
        close $fh;
    }
}