package uk.ac.ebi.age.service.submission;

import java.sql.Connection;

import uk.ac.ebi.age.transaction.ReadLock;
import uk.ac.ebi.mg.rwarbiter.Token;

public class SDBReadLock extends Token implements ReadLock
{
 private Connection connection;

 
 SDBReadLock()
 {
 }
 
 Connection getConnection()
 {
  return connection;
 }

 void setConnection(Connection connection)
 {
  this.connection = connection;
 }
}
