package uk.ac.ebi.age.storage;

import java.util.Map;

import uk.ac.ebi.age.model.ModuleKey;

import com.pri.util.Counter;

public class GlobalObjectConnection
{

 private ModuleKey               hostModuleKey;
 private Map<ModuleKey, Counter> incomingConnections;
 private String className;

 public void setClassName(String className)
 {
  this.className = className;
 }

 public ModuleKey getHostModuleKey()
 {
  return hostModuleKey;
 }

 public void setHostModuleKey(ModuleKey hostModuleKey)
 {
  this.hostModuleKey = hostModuleKey;
 }

 public Map<ModuleKey, Counter> getIncomingConnections()
 {
  return incomingConnections;
 }

 public void setIncomingConnections(Map<ModuleKey, Counter> incomingConnections)
 {
  this.incomingConnections = incomingConnections;
 }

 public String getClassName()
 {
  return className;
 }
 
 

}
