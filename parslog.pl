#########################################################################
#                                                     				    #
#  parslog 				                    		 Версия 1.00        #
#  Автор: Голдобин Михаил Дмитриевич                                    #
#  Дата созданиня: 04.11.2023 г.                                        #
#                                                                       #
#	       Парсим лог и формируем набор данных в СУБД PostgreSQL        #
#                                                                       #
#########################################################################

require 'text_error.pl';

use DBI;

#while ($arg = shift @ARGV){            
#	print "$arg\n";
#}

my $logfile = $ARGV[0];
print "Файл: $logfile\n";

my $SQL 	= '';
my $db_host = 'localhost';
my $db_port = 5432;
my $db_user = 'postgres';
my $db_pass = 'postgres';
my $db_name = 'postgres';
my $db 		= "dbi:Pg:dbname=${db_name};host=${db_host}";

my $dbh 	= DBI->connect("dbi:Pg:dbname=$db_name;host=$db_host;port=$db_port",  
                            $db_user,
                            $db_pass,
                            {AutoCommit => 0, RaiseError => 1}
						  ) or die $DBI::errstr;

$dbh->trace(1, 'tracelog.txt');

my $sth = $dbh->prepare('delete from public.message') or &error(__LINE__,$dbh->errstr);  
$sth->execute() or &error(__LINE__,$sth->errstr);	
#$sth->finish();
$dbh->commit;	

$sth = $dbh->prepare('delete from public.log') or &error(__LINE__,$dbh->errstr);  
$sth->execute() or &error(__LINE__,$sth->errstr);	
#$sth->finish();
$dbh->commit;

    if (!(-f "$logfile")) 			 		{&error(__LINE__,"$file_name is an invalid file name ($!).")}
    elsif (!(-r "$logfile"))                {&error(__LINE__,"$file_name is not readable ($!).")}
    elsif (!open(LOGFILE, "$logfile"))      {&error(__LINE__,"$file_name could not be opened ($!).")}   	
   
   my $countrecords  = 0;
   my $index_message = 0;
   my $count_message_fields = 0;
   
    while (<LOGFILE>) { 
		$countrecords = $countrecords + 1;
		
		$index_mess = index($_, "<=");
		#print $index_mess."\n";
		
		#строка прибытия сообщения 
		if ($index_mess >= 0){
			#print $_;
			#разбор строки через пробел
			my @message_fields = split(/ /, $_);
			#количество полей в строке
			$count_message_fields = @message_fields;
			#print "count_message_fields = ".$count_message_fields."\n";
			#случай, когда есть все требуемые поля: дата-время, id, int_id
			if ($count_message_fields == 10){
				my $created = $message_fields[0]." ".$message_fields[1];
				my $int_id = $message_fields[2];
				my @id_message = split(/=/, $message_fields[9]);
				#my $id = $id_message[1];
				my $id = substr($id_message[1], 0, index($id_message[1],"@"));
				#print "dt=".$created.", int_id=".$int_id.", id=".$id."\n";
				
				my $str = '';
				for (my $i=2; $i<10; $i++){
					if ($i == 2) {$str = $message_fields[$i];}
					else		 {$str = $str." ".$message_fields[$i]};
				}#for (my $i=0; $i<; $i++)
				my $status = 'true';				
				my $SQL = "INSERT INTO public.message VALUES(\'".$created."\'::timestamp without time zone, \'".$id."\', \'".$int_id."\', \'".$str."\', ".$status.");";
				#print $SQL."\n";
				$sth = $dbh->prepare($SQL) or &error(__LINE__,$dbh->errstr);  

				$sth->execute() or &error(__LINE__,$sth->errstr);	
				#$sth->finish();
				$dbh->commit;				
			}#if ($count_message_fields == 10)			
		}#if ($index_mess >= 0)
		else {
				#разбор строки через пробел
				my @message_fields = split(/ /, $_);
				$count_message_fields = @message_fields;
				my $created = $message_fields[0]." ".$message_fields[1];
				my $int_id = $message_fields[2];
				my @id_message = split(/=/, $message_fields[9]);
				#my $id = $id_message[1];
				#my $id = substr($id_message[1], 0, index($id_message[1],"@"));
				#print "dt=".$created.", int_id=".$int_id.", id=".$id."\n";
				
				my $str = '';
				for (my $i=2; $i<$count_message_fields; $i++){
					if ($i == 2) {$str = $message_fields[$i];}
					else		 {$str = $str." ".$message_fields[$i]};
				}#for (my $i=0; $i<; $i++)
				#в тексте сообщения встречается "'", что в запросе не пройдет. заменим "'" на "`" 				
				$str =~ s/\'/\`/g;
				my $address = $message_fields[4];				
				my $SQL = "INSERT INTO public.log VALUES(\'".$created."\'::timestamp without time zone, \'".$int_id."\', \'".$str."\', \'".$address."\');";
				#print $SQL."\n";
				#print $str."\n";
				my $sth = $dbh->prepare($SQL) or &error(__LINE__,$dbh->errstr);  

				$sth->execute() or &error(__LINE__,$sth->errstr);	
				$dbh->commit;				
		}#else if ($index_mess >= 0) 
    }#while (<LOGFILE>)

    close(LOGFILE);
	
	$dbh->disconnect;
	
	&message("Ok",$countrecords);

    &error("Ok","Работа завершена");
	
1;