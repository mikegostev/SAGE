package uk.ac.ebi.age.storage.index;

import uk.ac.ebi.age.model.AgeObject;
import uk.ac.ebi.age.query.AgeQuery;

public interface AgeIndexWritable
{
 boolean isDirty();
 void setDirty( boolean dirty );
 
 AgeQuery getQuery();

 void index(Iterable<? extends AgeObject> res, boolean append);

 void close();
}
