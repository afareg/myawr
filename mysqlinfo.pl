use strict;
use warnings;
use Getopt::Long;

Getopt::Long::Configure qw(no_ignore_case);    #


( my $sript_name = $0 ) =~ s!.*/(.*)!$1! ;
my $is_running = `ps -ef | grep $sript_name | grep -v grep | wc -l`;
exit if($is_running>2) ;

my %opt;                                       # Get options info

my $port = 3306;                               # -P
my $user = "user";
my $pswd;
my $host;

my $tport = 3306;                              # -P
my $tuser = "user";
my $tpswd;
my $thost;

my $snap_time =`date +"%Y-%m-%d %H:%M:%S"`;
my $snap_id=1;

# Get options info
&get_options();
&init();
&get_status;
&get_tables;
&get_columns;
&get_processlist;

sub init {

	use DBI;
	use DBI qw(:sql_types);

	my $dbh_save = DBI->connect( "DBI:mysql:database=mdbutil;host=$thost;port=$tport","$tuser", "$tpswd", { 'RaiseError' => 0 ,AutoCommit => 1} );
	if(not $dbh_save) {
		return;
	}

 	my $sth = $dbh_save->prepare("SELECT ifnull(max(s_id),0)+1 from snapshot where s_host='$host' and s_port=$port");
	$sth->execute();
	$snap_id=$sth->fetchrow_array();

    my $sql =qq{insert into snapshot(s_id,s_host,s_port,s_time) values($snap_id, \"$host\",$port,\"$snap_time\")};
    $dbh_save->do($sql);

	$sth->finish;
	$dbh_save->disconnect();
}

sub get_processlist {
	use DBI;
	use DBI qw(:sql_types);

	my $dbh = DBI->connect( "DBI:mysql:database=information_schema;host=$host;port=$port","$user", "$pswd", { 'RaiseError' => 0 } );
	if(not $dbh) {
		return;
	}
	my $dbh_save = DBI->connect( "DBI:mysql:database=mdbutil;host=$thost;port=$tport","$tuser", "$tpswd", { 'RaiseError' => 0 ,AutoCommit => 0} );
	if(not $dbh_save) {
		return;
	}
	my $sql2 = qq{ insert into processlist values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) };
	my $sth2 = $dbh_save->prepare( $sql2 );

	my $sth = $dbh->prepare("select * from processlist ");
	$sth->execute();
	while( my @result2 = $sth->fetchrow_array )	{
			$sth2->bind_param( 1, $snap_id, SQL_INTEGER );
			$sth2->bind_param( 2, $host, SQL_VARCHAR);
			$sth2->bind_param( 3, $port, SQL_INTEGER );
			$sth2->bind_param( 4, $snap_time, SQL_DATE );
			$sth2->bind_param( 5, $result2[0], SQL_VARCHAR );
			$sth2->bind_param( 6, $result2[1], SQL_VARCHAR );
			$sth2->bind_param( 7, $result2[2], SQL_VARCHAR );
			$sth2->bind_param( 8, $result2[3], SQL_VARCHAR );
			$sth2->bind_param( 9, $result2[4], SQL_VARCHAR );
			$sth2->bind_param(10, $result2[5], SQL_VARCHAR );
			$sth2->bind_param(11, $result2[6], SQL_VARCHAR );
			$sth2->bind_param(12, $result2[7], SQL_VARCHAR );
			$sth2->bind_param(13, $result2[8], SQL_VARCHAR );
			$sth2->bind_param(14, $result2[9], SQL_VARCHAR );
			$sth2->bind_param(15, $result2[10], SQL_VARCHAR );
			$sth2->execute();
			$dbh_save->commit();
	}
	$sth2->finish;
	$dbh_save->disconnect();
    $sth->finish;
	$dbh->disconnect();
}

sub get_columns {
	use DBI;
	use DBI qw(:sql_types);

	my $dbh = DBI->connect( "DBI:mysql:database=information_schema;host=$host;port=$port","$user", "$pswd", { 'RaiseError' => 0 } );
	if(not $dbh) {
		return;
	}
	my $dbh_save = DBI->connect( "DBI:mysql:database=mdbutil;host=$thost;port=$tport","$tuser", "$tpswd", { 'RaiseError' => 0 ,AutoCommit => 0} );
	if(not $dbh_save) {
		return;
	}
	my $sql2 = qq{ insert into columns values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) };
	my $sth2 = $dbh_save->prepare( $sql2 );

	my $sth = $dbh->prepare("select * from columns where TABLE_SCHEMA not in ('information_schema','mysql','performance_schema','sys','test')");
	$sth->execute();
	while( my @result2 = $sth->fetchrow_array )	{
			$sth2->bind_param( 1, $snap_id, SQL_INTEGER );
			$sth2->bind_param( 2, $host, SQL_VARCHAR);
			$sth2->bind_param( 3, $port, SQL_INTEGER );
			$sth2->bind_param( 4, $snap_time, SQL_DATE );
			$sth2->bind_param( 5, $result2[0], SQL_VARCHAR );
			$sth2->bind_param( 6, $result2[1], SQL_VARCHAR );
			$sth2->bind_param( 7, $result2[2], SQL_VARCHAR );
			$sth2->bind_param( 8, $result2[3], SQL_VARCHAR );
			$sth2->bind_param( 9, $result2[4], SQL_VARCHAR );
			$sth2->bind_param(10, $result2[5], SQL_VARCHAR );
			$sth2->bind_param(11, $result2[6], SQL_VARCHAR );
			$sth2->bind_param(12, $result2[7], SQL_VARCHAR );
			$sth2->bind_param(13, $result2[8], SQL_VARCHAR );
			$sth2->bind_param(14, $result2[9], SQL_VARCHAR );
			$sth2->bind_param(15, $result2[10], SQL_VARCHAR );
			$sth2->bind_param(16, $result2[11], SQL_VARCHAR );
			$sth2->bind_param(17, $result2[12], SQL_VARCHAR );
			$sth2->bind_param(18, $result2[13], SQL_VARCHAR );
			$sth2->bind_param(19, $result2[14], SQL_VARCHAR );
			$sth2->bind_param(20, $result2[15], SQL_VARCHAR );
			$sth2->bind_param(21, $result2[16], SQL_VARCHAR );
			$sth2->bind_param(22, $result2[17], SQL_VARCHAR );
			$sth2->bind_param(23, $result2[18], SQL_VARCHAR );
			$sth2->bind_param(24, $result2[19], SQL_VARCHAR );
			$sth2->bind_param(25, $result2[20], SQL_VARCHAR );
			$sth2->execute();
			$dbh_save->commit();
	}
	$sth2->finish;
	$dbh_save->disconnect();
    $sth->finish;
	$dbh->disconnect();
}

sub get_tables {
	use DBI;
	use DBI qw(:sql_types);

	my $dbh = DBI->connect( "DBI:mysql:database=information_schema;host=$host;port=$port","$user", "$pswd", { 'RaiseError' => 0 } );
	if(not $dbh) {
		return;
	}
	my $dbh_save = DBI->connect( "DBI:mysql:database=mdbutil;host=$thost;port=$tport","$tuser", "$tpswd", { 'RaiseError' => 0 ,AutoCommit => 0} );
	if(not $dbh_save) {
		return;
	}
	my $sql2 = qq{ insert into tables values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) };
	my $sth2 = $dbh_save->prepare( $sql2 );

	my $sth = $dbh->prepare("select * from tables where TABLE_SCHEMA not in ('information_schema','mysql','performance_schema','sys','test')");
	$sth->execute();
	while( my @result2 = $sth->fetchrow_array )	{
			$sth2->bind_param( 1, $snap_id, SQL_INTEGER );
			$sth2->bind_param( 2, $host, SQL_VARCHAR);
			$sth2->bind_param( 3, $port, SQL_INTEGER );
			$sth2->bind_param( 4, $snap_time, SQL_DATE );
			$sth2->bind_param( 5, $result2[0], SQL_VARCHAR );
			$sth2->bind_param( 6, $result2[1], SQL_VARCHAR );
			$sth2->bind_param( 7, $result2[2], SQL_VARCHAR );
			$sth2->bind_param( 8, $result2[3], SQL_VARCHAR );
			$sth2->bind_param( 9, $result2[4], SQL_VARCHAR );
			$sth2->bind_param(10, $result2[5], SQL_VARCHAR );
			$sth2->bind_param(11, $result2[6], SQL_VARCHAR );
			$sth2->bind_param(12, $result2[7], SQL_VARCHAR );
			$sth2->bind_param(13, $result2[8], SQL_VARCHAR );
			$sth2->bind_param(14, $result2[9], SQL_VARCHAR );
			$sth2->bind_param(15, $result2[10], SQL_VARCHAR );
			$sth2->bind_param(16, $result2[11], SQL_VARCHAR );
			$sth2->bind_param(17, $result2[12], SQL_VARCHAR );
			$sth2->bind_param(18, $result2[13], SQL_VARCHAR );
			$sth2->bind_param(19, $result2[14], SQL_VARCHAR );
			$sth2->bind_param(20, $result2[15], SQL_VARCHAR );
			$sth2->bind_param(21, $result2[16], SQL_VARCHAR );
			$sth2->bind_param(22, $result2[17], SQL_VARCHAR );
			$sth2->bind_param(23, $result2[18], SQL_VARCHAR );
			$sth2->bind_param(24, $result2[19], SQL_VARCHAR );
			$sth2->bind_param(25, $result2[20], SQL_VARCHAR );
			$sth2->execute();
			$dbh_save->commit();
	}
	$sth2->finish;
	$dbh_save->disconnect();
    $sth->finish;
	$dbh->disconnect();
}

sub get_status {
	use DBI;
	use DBI qw(:sql_types);

	my $dbh = DBI->connect( "DBI:mysql:database=information_schema;host=$host;port=$port","$user", "$pswd", { 'RaiseError' => 0 } );
	if(not $dbh) {
		return;
	}
	my $dbh_save = DBI->connect( "DBI:mysql:database=mdbutil;host=$thost;port=$tport","$tuser", "$tpswd", { 'RaiseError' => 0 ,AutoCommit => 0} );
	if(not $dbh_save) {
		return;
	}
	my $sql2 = qq{ insert into global_status(s_id,s_host,s_port,s_time,VARIABLE_NAME,VARIABLE_VALUE) values (?, ?,?,?,?, ?) };
	my $sth2 = $dbh_save->prepare( $sql2 );

	my $sth = $dbh->prepare("show global status");
	$sth->execute();
	while( my @result2 = $sth->fetchrow_array )	{
			$sth2->bind_param( 1, $snap_id, SQL_INTEGER );
			$sth2->bind_param( 2, $host, SQL_VARCHAR);
			$sth2->bind_param( 3, $port, SQL_INTEGER );
			$sth2->bind_param( 4, $snap_time, SQL_DATE );
			$sth2->bind_param( 5, $result2[0], SQL_VARCHAR );
			$sth2->bind_param( 6, $result2[1], SQL_VARCHAR );
			$sth2->execute();
			$dbh_save->commit();
	}
	$sth2->finish;
	$dbh_save->disconnect();
    $sth->finish;
	$dbh->disconnect();
}

# ----------------------------------------------------------------------------------------
#
# Func :  print usage
# ----------------------------------------------------------------------------------------
sub print_usage {

	#print BLUE(),BOLD(),<<EOF,RESET();
	print <<EOF;

==========================================================================================
Info  :
Usage :
Command line options :
   -help,--help           Print Help Info. t

   -P,--port           Port number to use for local mysql connection(default 3306).
   -u,--user           user name for local mysql(default user).
   -p,--pswd           user password for local mysql(can't be null).
   -h,--host          localhost(ip) for mysql where info is got(can't be null).

   -tP,--tport          Port number to use for  mysql where info is saved (default 3306).
   -tu,--tuser          user name for  mysql where info is saved(default user).
   -tp,--pswd           user password for mysql where info is saved(can't be null).
   -th,--thost          host(ip) for mysql where info is saved(can't be null).t

Sample :
   shell> perl mysqlinfo.pl -p 111111 -h 192.168.1.111 -tp 111111 -th 192.168.1.200  ß
==========================================================================================
EOF
	exit;
}

# ----------------------------------------------------------------------------------------
#
# Func : get options and set option flag
# ----------------------------------------------------------------------------------------
sub get_options {

	# Get options info
	GetOptions(
		\%opt,
		'help|help',       # OUT : print help info
		'h|host=s',        # IN  : host
		'P|port=i',        # IN  : port
		'u|user=s',        # IN  : user
		'p|pswd=s',        # IN  : password
		'tu|tuser=s',      # IN  : user
		'tp|tpswd=s',      # IN  : password
		'th|thost=s',      # IN  : host
		'tP|tport=i',      # IN  : port
	) or print_usage();

	if ( !scalar(%opt) ) {
		&print_usage();
	}

	# Handle for options
	$opt{'help'}  and print_usage();
	$opt{'h'}  and $host = $opt{'h'};
	$opt{'P'}  and $port = $opt{'P'};
	$opt{'u'}  and $user = $opt{'u'};
	$opt{'p'}  and $pswd = $opt{'p'};
	$opt{'th'} and $thost = $opt{'th'};
	$opt{'tP'} and $tport = $opt{'tP'};
	$opt{'tu'} and $tuser = $opt{'tu'};
	$opt{'tp'} and $tpswd = $opt{'tp'};

	if (
		!(
			    defined $host
			and defined $thost
		)
	  )
	{
		&print_usage();
	}
}