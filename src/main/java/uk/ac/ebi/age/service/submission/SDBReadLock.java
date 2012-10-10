package uk.ac.ebi.age.service.submission;

import java.sql.Statement;

import uk.ac.ebi.mg.rwarbiter.Token;

public class SDBReadLock extends Token
{
 SDBReadLock()
 {
 }
 
 private Statement stmt;
 
 Statement getStatement()
 {
  return stmt;
 }

 void setStatement(Statement stmt)
 {
  this.stmt = stmt;
 }
}
