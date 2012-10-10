package uk.ac.ebi.age.mng.submission;

import java.util.HashMap;
import java.util.Map;

import uk.ac.ebi.age.ext.submission.DataModuleMeta;
import uk.ac.ebi.age.model.writable.AgeObjectWritable;
import uk.ac.ebi.age.model.writable.DataModuleWritable;

public class ModMeta
{
 DataModuleWritable             origModule;
 DataModuleWritable             newModule;
 DataModuleMeta                 meta;
 ModuleAux                      aux;

 Map<String, AgeObjectWritable> idMap = new HashMap<String, AgeObjectWritable>();
}
