package uk.ac.ebi.age.service.submission;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import uk.ac.ebi.age.transaction.Transaction;

import com.pri.util.Pair;



public final class SDBTransaction extends SDBReadLock implements Transaction
{
 private boolean prepared;
 private String transactionId;
 private List<Pair<Path, Path>> links;
 
 SDBTransaction()
 {
 }



 boolean isPrepared()
 {
  return prepared;
 }

 void setPrepared(boolean prepared)
 {
  this.prepared = prepared;
 }

 String getTransactionId()
 {
  return transactionId;
 }

 void setTransactionId(String transactionId)
 {
  this.transactionId = transactionId;
 }

 void addLink(Path from, Path to)
 {
  if( links == null )
   links = new ArrayList<>();
 
  links.add(new Pair<>(from,to));
 }
 
 List<Pair<Path, Path>> getLinks()
 {
  return links;
 }

 void setLinks(List<Pair<Path, Path>> links)
 {
  this.links = links;
 }
 
 
}
