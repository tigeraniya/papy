#!/usr/bin/env perl
# $Id: dalilite.pl 877 2009-04-14 15:31:32Z hpm $
# ======================================================================
# WSDaliLite Perl client.
#
# Requires SOAP::Lite. Tested with versions 0.60, 0.69 and 0.71.
#
# See:
# http://www.ebi.ac.uk/Tools/Webservices/services/dalilite
# http://www.ebi.ac.uk/Tools/Webservices/clients/dalilite
# http://www.ebi.ac.uk/Tools/Webservices/tutorials/soaplite
# ======================================================================
# WSDL URL for service
my $WSDL = 'http://www.ebi.ac.uk/Tools/webservices/wsdl/WSDaliLite.wsdl';

# Enable Perl warnings
use strict;
use warnings;

# Load libraries
use SOAP::Lite;
use Data::Dumper;
use Getopt::Long qw(:config no_ignore_case bundling);
use File::Basename;

# Set interval for checking status
my $checkInterval = 5;

# Output level
my $outputLevel = 1;

# Process command-line options
my $numOpts = scalar(@ARGV);
my (
	$help,  $async, $outfile,  $outformat, $polljob, $status,
	$jobid, $trace, $sequence, $quiet,     $verbose
);
my %params = (
	'async' => '1',    # Use async mode and simulate sync mode in client
);
GetOptions(
	'pdb1=s'        => \$params{'sequence1'},    # PDB file or ID 1
	'chainid1=s'    => \$params{'chainid1'},     # Chain in PDB 1
	'pdb2=s'        => \$params{'sequence2'},    # PDB file or ID 2
	'chainid2=s'    => \$params{'chainid2'},     # Chain in PDB 2
	"help|h"        => \$help,                   # Usage info
	"async|a"       => \$async,                  # Asynchronous submission
	'outfile|O=s'   => \$outfile,                # Output file name
	'outformat|o=s' => \$outformat,              # Output file type
	"polljob"       => \$polljob,                # Get results
	"status"        => \$status,                 # Get status
	"jobid|j=s"     => \$jobid,                  # JobId
	"email|S=s"     => \$params{'email'},        # E-mail address
	'quiet|q'       => \$quiet,                  # Decrease output level
	'verbose|v'     => \$verbose,                # Increase output level
	'WSDL=s'        => \$WSDL,                   # Alternative WSDL URL
	"trace"         => \$trace,                  # SOAP messages
);
if ($verbose) { $outputLevel++ }
if ($quiet)   { $outputLevel-- }

# Get the script filename for use in usage messages
my $scriptName = basename( $0, () );

# Print usage and exit if requested
if ( $help || $numOpts == 0 ) {
	&usage();
	exit(0);
}

# If required enable SOAP message trace
if ($trace) {
	print "Tracing active\n";
	SOAP::Lite->import( +trace => 'debug' );
}

# Create the service interface, setting the fault handler to throw exceptions
my $soap = SOAP::Lite->service($WSDL)->proxy(
	'http://localhost/',

	#proxy => ['http' => 'http://your.proxy.server/'], # HTTP proxy
	timeout => 600,    # HTTP connection timeout
  )->on_fault(
	sub {
		my $soap = shift;
		my $res  = shift;

		# Throw an exception for all faults
		if ( ref($res) eq '' ) {
			die($res);
		}
		else {
			die( $res->faultstring );
		}
		return new SOAP::SOM;
	}
  );

# Print usage if bad argument combination
if (   !( $polljob || $status )
	&& !( defined( $params{'sequence1'} ) && defined( $params{'sequence2'} ) ) )
{
	print STDERR 'Error: bad option combination', "\n";
	&usage();
	exit(1);
}

# Poll job and get results
elsif ( $polljob && defined($jobid) ) {
	if ( $outputLevel > 1 ) {
		print "Getting results for job $jobid\n";
	}
	&getResults($jobid);
}

# Job status
elsif ( $status && defined($jobid) ) {
	if ( $outputLevel > 0 ) {
		print STDERR "Getting status for job $jobid\n";
	}
	my $result = $soap->checkStatus($jobid);
	print STDOUT "$result", "\n";
	if ( $result eq 'DONE' && $outputLevel > 0 ) {
		print STDERR "To get results: $scriptName --polljob --jobid $jobid\n";
	}
}

# Submit a job
else {
	if ( defined( $params{'sequence1'} ) && -f $params{'sequence1'} ) {
		$params{'sequence1'} = &read_file( $params{'sequence1'} );
	}
	if ( defined( $params{'sequence2'} ) && -f $params{'sequence2'} ) {
		$params{'sequence2'} = &read_file( $params{'sequence2'} );
	}

	my $paramsData = SOAP::Data->name('params')->type( map => \%params );

	# For SOAP::Lite 0.60 and earlier parameters are passed directly
	if ( $SOAP::Lite::VERSION eq '0.60' || $SOAP::Lite::VERSION =~ /0\.[1-5]/ ) {
		$jobid = $soap->runDaliLite($paramsData);
	}

	# For SOAP::Lite 0.69 and later parameter handling is different, so pass
	# undef's for templated params, and then pass the formatted args.
	else {
		$jobid = $soap->runDaliLite( undef, $paramsData );
	}

	if ( defined($async) ) {
		print STDOUT $jobid, "\n";
		if ( $outputLevel > 0 ) {
			print STDERR
			  "To check status: $scriptName --status --jobid $jobid\n";
		}
	}
	else {    # Synchronous mode
		if ( $outputLevel > 0 ) {
			print STDERR "JobId: $jobid\n";
		}
		sleep 1;
		&getResults($jobid);
	}
}

# Client-side poll
sub clientPoll($) {
	my $jobid  = shift;
	my $result = 'PENDING';

	# Check status and wait if not finished
	#print STDERR "Checking status: $jobid\n";
	while ( $result eq 'RUNNING' || $result eq 'PENDING' ) {
		$result = $soap->checkStatus($jobid);
		if ( $outputLevel > 0 ) {
			print STDERR "$result\n";
		}
		if ( $result eq 'RUNNING' || $result eq 'PENDING' ) {

			# Wait before polling again.
			sleep $checkInterval;
		}
	}
}

# Get the results for a jobid
sub getResults($) {
	my $jobid = shift;

	# Check status, and wait if not finished
	clientPoll($jobid);

	# Use JobId if output file name is not defined
	unless ( defined($outfile) ) {
		$outfile = $jobid;
	}

	# Get list of data types
	my $resultTypes = $soap->getResults($jobid);

	# Get the data and write it to a file
	if ( defined($outformat) ) {    # Specified data type
		my $selResultType;
		foreach my $resultType (@$resultTypes) {
			if ( $resultType->{type} eq $outformat ) {
				$selResultType = $resultType;
			}
		}
		if ( defined($selResultType) ) {
			my $res = $soap->poll( $jobid, $selResultType->{type} );
			if ( $outfile eq '-' ) {
				write_file( $outfile, $res );
			}
			else {
				write_file( $outfile . '.' . $selResultType->{ext}, $res );
			}
		}
		else {
			die "Error: unknown result format \"$outformat\"";
		}
	}
	else {    # Data types available
		      # Write a file for each output type
		for my $resultType (@$resultTypes) {
			if ( $outputLevel > 1 ) {
				print STDERR "Getting $resultType->{type}\n";
			}
			my $res = $soap->poll( $jobid, $resultType->{type} );
			if ( $outfile eq '-' ) {
				write_file( $outfile, $res );
			}
			else {
				write_file( $outfile . '.' . $resultType->{ext}, $res );
			}
		}
	}
}

# Read a file
sub read_file($) {
	my $filename = shift;
	open( FILE, $filename );
	my $content;
	my $buffer;
	while ( sysread( FILE, $buffer, 1024 ) ) {
		$content .= $buffer;
	}
	close(FILE);
	return $content;
}

# Write a result file
sub write_file($$) {
	my ( $filename, $data ) = @_;
	if ( $outputLevel > 0 ) {
		print STDERR 'Creating result file: ' . $filename . "\n";
	}
	if ( $filename eq '-' ) {
		print STDOUT $data;
	}
	else {
		open( FILE, ">$filename" )
		  or die "Error: unable to open output file $filename ($!)";
		syswrite( FILE, $data );
		close(FILE);
	}
}

# Print program usage
sub usage {
	print STDERR <<EOF
DaliLite
========

Pairwise comparison of protein structures

[Required]

  --pdb1            : str  : PDB ID for structure 1
  --pdb2            : str  : PDB ID for structure 2

[Optional]

  --chainid1        : str  : Chain identifer in structure 1
  --chainid2        : str  : Chain identifer in structure 2

[General]

  -h, --help        :      : prints this help text
  -S, --email       : str  : user email address
  -a, --async       :      : asynchronous submission
      --status      :      : poll for the status of a job
      --polljob     :      : poll for the results of a job
  -j, --jobid       : str  : jobid for an asynchronous job
  -O, --outfile     : str  : file name for results (default is jobid;
                             "-" for STDOUT)
  -o, --outformat   : str  : result format to retrieve
  -q, --quiet       :      : decrease output
  -v, --verbose     :      : increase output
      --trace	    :      : show SOAP messages being interchanged 

Synchronous job:

  The results/errors are returned as soon as the job is finished.
  Usage: $scriptName --email <your\@email> [options] pdbFile [--outfile string]
  Returns: saves the results to disk

Asynchronous job:

  Use this if you want to retrieve the results at a later time. The results 
  are stored for up to 24 hours. 
  The asynchronous submission mode is recommended when users are submitting 
  batch jobs or large database searches	
  Usage: $scriptName --email <your\@email> --async [options] pdbFile
  Returns: jobid

  Use the jobid to query for the status of the job. 
  Usage: $scriptName --status --jobid <jobId>
  Returns: string indicating the status of the job:
    DONE - job has finished
    RUNNING - job is running
    NOT_FOUND - job cannot be found
    ERROR - the jobs has encountered an error

  When done, use the jobid to retrieve the status of the job. 
  Usage: $scriptName --polljob --jobid <jobId> [--outfile string]

[Help]

  For more detailed help information refer to
  http://www.ebi.ac.uk/DaliLite/
EOF
	  ;
}
