package uk.ac.ebi.age.storage.impl.serswap.v3;

import uk.ac.ebi.age.model.AgeObject;
import uk.ac.ebi.age.model.AttributeClassRef;
import uk.ac.ebi.age.model.impl.v3.AgeIntegerAttributeImpl;
import uk.ac.ebi.age.model.writable.AgeAttributeWritable;
import uk.ac.ebi.age.model.writable.AgeObjectWritable;
import uk.ac.ebi.age.model.writable.AttributedWritable;

public class SwapIntegerAttribute extends AgeIntegerAttributeImpl
{

 private static final long serialVersionUID = 3L;

 protected SwapIntegerAttribute(AttributeClassRef attrClass, AttributedWritable host)
 {
  super(attrClass, host);
 }
 
 @Override
 public AttributedWritable getAttributedHost()
 {
  AttributedWritable host = super.getAttributedHost();

  if( host instanceof AgeObjectWritable && ! (host instanceof AgeObjectProxy) )
  {
   host = ((SwapDataModuleImpl)((AgeObject)host).getDataModule()).getModuleRef().getObjectProxy( ((AgeObject)host).getId() );
   
   setAttributedHost(host);
  }

  return host;
 }
 
 @Override
 public AgeObjectProxy getMasterObject()
 {
  AgeObjectWritable host = super.getMasterObject();
  
  if( host instanceof AgeObjectProxy)
   return (AgeObjectProxy)host;
  
  AgeObjectProxy pxo = ((SwapDataModuleImpl)((AgeObject)host).getDataModule()).getModuleRef().getObjectProxy( host.getId() );
  
  setAttributedHost(pxo);
  
  return pxo;
 }
 
 @Override
 public AgeAttributeWritable createClone( AttributedWritable host )
 {
  SwapIntegerAttribute clone  = new SwapIntegerAttribute(getClassReference(), host);
  
  clone.setIntValue(getValueAsInteger());
  
  cloneAttributes( clone );

  return clone;
 }
}
