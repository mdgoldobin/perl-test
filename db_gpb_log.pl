#!c:\Perl64\bin\perl

####################################################################################
#                                                     				    		   #
#  db_gpb_log			                     	 Версия 1.00        		       #
#  Автор: Голдобин Михаил Дмитриевич                                    		   #
#  Дата созданиня: 05.11.2023 г.                                        		   #
#                                                                       		   #
#  Запрос к СУБД PostgreSQL, выборка данных по адресу, формирование HTML-страницы  #
#                                                                       		   #
####################################################################################

use DBI;
require "cgi-lib.pl";

my $address = 'empty';

if (&ReadParse(*input)) {
	$address  = $input{'address'};
} else {

}

my $db_host = 'localhost';
my $db_port =  5432;
my $db_user = 'postgres';
my $db_pass = 'postgres';
my $db_name = 'postgres';
my $db 		= "dbi:Pg:dbname=${db_name};host=${db_host}";
my $dbh 	= DBI->connect($db, $db_user, $db_pass, {RaiseError => 1, AutoCommit => 0}) 
							or die "Error connecting to the database: $DBI::errstr\n";

my $query 	= "select now()::timestamp without time zone";
my $dt_now 	= $dbh->selectrow_array($query);

my $query = "select count(*) from (\n".
			"	select created, str from public.message where str like \'%".$address."%\'\n".
			"	UNION \n".
			"	select created, str from public.log where address like \'%".$address."%\') all_log";		

my $count 	= $dbh->selectrow_array($query);

$query = "select created, str from (\n".
			"	select created, str, int_id from public.message where str like \'%".$address."%\'\n".
			"	UNION \n".
			"	select created, str, int_id from public.log where address like \'%".$address."%\') all_log\n".
			" order by int_id, created limit 100";	

my $sth 	= $dbh->prepare($query);  
$sth->execute();

print "Content-type: text/html; charset=UTF-8\n\n";
print "<html>\n";
print "<head>\n";
print "<meta http-equiv=\"Content-Type\" content=\"text/html\" charset=UTF-8>\n";
print "<title>Результаты запроса</title>\n";

print "<script>\n";
print "function showAlert() {\n";
print "alert (\"Показаны первые 100 записей. Количество записей всего: $count\")\n";
print "}\n";
print "</script>\n";

print "</head>\n";
if ($count > 100){print "<body onload=\"showAlert()\">\n";}
else {print "<body>\n";} 
print "<h1 align=center>Результаты запроса к БД</h1>";
print "<h5>Дата и время обращения к БД: $dt_now</h5>";
print "<h2>Запрос на вхождение в строку лога или в адрес: $address</h2>";
print "<table border=\"1\" cellpadding=\"7\" cellspacing=\"0\" width=1800>\n";
print "<tr><td align=center>Дата и время</td><td align=center>Строка лога\</td></tr>";

while (my @row = $sth->fetchrow_array) { 	
	print "<tr><td>$row[0]</td><td>$row[1]</td></tr>";
}#while

$sth->finish();

$dbh->disconnect;

print "</table>\n";
print "</body>\n";
print "</html>\n";

1;

