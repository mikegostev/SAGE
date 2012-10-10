package uk.ac.ebi.age.mng.submission;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.ebi.age.ext.log.LogNode;
import uk.ac.ebi.age.ext.log.LogNode.Level;
import uk.ac.ebi.age.ext.submission.DataModuleMeta;
import uk.ac.ebi.age.ext.submission.Factory;
import uk.ac.ebi.age.ext.submission.FileAttachmentMeta;
import uk.ac.ebi.age.ext.submission.Status;
import uk.ac.ebi.age.ext.submission.SubmissionMeta;
import uk.ac.ebi.age.model.AgeRelationClass;
import uk.ac.ebi.age.model.ModuleKey;
import uk.ac.ebi.age.model.RelationClassRef;
import uk.ac.ebi.age.model.writable.AgeObjectWritable;
import uk.ac.ebi.age.model.writable.DataModuleWritable;
import uk.ac.ebi.age.service.submission.SubmissionDB;
import uk.ac.ebi.age.storage.AgeStorageAdm;
import uk.ac.ebi.age.storage.ConnectionInfo;

public class SubmissionSession
{
 private final  List<ModMeta> incomingMods = new ArrayList<ModMeta>(); //New modules and modules with data update (Ins+Upd) but in original order
 
 private final  List<ModMeta> mod4Use = new ArrayList<ModMeta>(); //Ins+Upd+Hld+MetaUpd convenience map

 private final  List<ModMeta> mod4Ins = new ArrayList<ModMeta>();
 
 private final  Map<String,ModMeta> mod4MetaUpd = new HashMap<String, ModMeta>(); //Modules with meta (description) update only
 private final  Map<String,ModMeta> mod4DataUpd = new HashMap<String, ModMeta>(); //Modules with data update
 private final  Map<String,ModMeta> mod4Del = new HashMap<String, ModMeta>(); //Modules to be deleted
 private final  Map<String,ModMeta> mod4Hld = new HashMap<String, ModMeta>(); //Modules to be retained (fully untouched, even meta)
 private final  Map<String,ModMeta> mod4DataHld = new HashMap<String, ModMeta>(); //Modules with data to be retained (mod4Hld+mod4MetaUpd)

 private final  Map<String,FileAttachmentMeta> att4Ins = new HashMap<String, FileAttachmentMeta>(); //New files
 private final  Map<String,FileMeta> att4Upd = new HashMap<String, FileMeta>(); //Files with content update
 private final  Map<String,FileMeta> att4MetaUpd = new HashMap<String, FileMeta>(); //Files with meta update only
 private final  Map<String,FileAttachmentMeta> att4Del = new HashMap<String, FileAttachmentMeta>();
 private final  Map<String,FileMeta> att4G2L = new HashMap<String, FileMeta>(); //Files that reduce visibility
 private final  Map<String,FileMeta> att4L2G = new HashMap<String, FileMeta>(); //Files that increase visibility
 private final  Map<String,FileAttachmentMeta> att4Hld = new HashMap<String, FileAttachmentMeta>(); //Files that keep both content and visibility untouched
 private final  Map<String,FileAttachmentMeta> att4Use = new HashMap<String, FileAttachmentMeta>(); //New full file set

 private String                           id;

 private final  Map<String, AgeObjectWritable>          clusterIdMap   = new HashMap<String, AgeObjectWritable>();
 private final  Map<String, AgeObjectWritable>          newGlobalIdMap = new HashMap<String, AgeObjectWritable>();
 private final  Map<String, AgeObjectWritable>          obsoleteGlobalIdMap = new HashMap<String, AgeObjectWritable>();

 private final  Map<AgeRelationClass, RelationClassRef> relRefMap      = new HashMap<AgeRelationClass, RelationClassRef>();
 
 private final  ConnectionInfo connInfo = new ConnectionInfo(); 

 private final AgeStorageAdm ageStorage;
 private final SubmissionDB submissionDB;

 public SubmissionSession( AgeStorageAdm st, SubmissionDB sbm )
 {
  ageStorage = st;
  submissionDB = sbm;
 }
 
 boolean prepare(SubmissionMeta sMeta, SubmissionMeta origSbm, LogNode logRoot)
 {
  char[] tmpId = new char[] { '\0', '\0' };

  if(sMeta.getId() == null)
   id = new String(tmpId);
  else
   id = sMeta.getId();

  List<FileAttachmentMeta> files = sMeta.getAttachments();

  boolean res = true;

  int n = 0;

  if(sMeta.getDataModules() != null)
  {
   for(DataModuleMeta dm : sMeta.getDataModules()) // Separating modules
   {

    n++;

    ModuleAux modAux = (ModuleAux) dm.getAux();

    DataModuleWritable exstMod = dm.getId() != null && sMeta.getStatus() == Status.UPDATE ? ageStorage.getDataModule(new ModuleKey(sMeta.getId(), dm.getId()) ) : null;

    if(modAux.getStatus() == Status.UPDATEORNEW)
    {
     if(exstMod == null)
      modAux.setStatus(Status.NEW);
     else
      modAux.setStatus(Status.UPDATE);
    }

    if(modAux.getStatus() == Status.UPDATE || modAux.getStatus() == Status.DELETE)
    {
     if(sMeta.getStatus() != Status.UPDATE)
     {
      logRoot.log(Level.ERROR, "Module " + modAux.getOrder() + " is marked for " + modAux.getStatus().name() + " but submission is not in UPDATE mode");
      res = false;
      continue;
     }

     if(exstMod == null)
     {
      logRoot.log(Level.ERROR, "The storage doesn't contain data module with ID='" + dm.getId() + "' (Cluster ID='" + sMeta.getId()
        + "') to be updated/deleted");
      res = false;
     }

     if(dm.getId() == null)
     {
      logRoot.log(Level.ERROR, "Module " + modAux.getOrder() + " is marked for " + modAux.getStatus().name() + " but no ID is provided");
      res = false;
      continue;
     }

     ModMeta mm = new ModMeta();
     mm.meta = dm;
     mm.aux = modAux;

     mm.origModule = exstMod;

     if(mm.aux.getStatus() == Status.DELETE)
     {
      mod4Del.put(mm.meta.getId(), mm);
     }
     else if(dm.getText() != null)
     {
      mod4DataUpd.put(mm.meta.getId(), mm);
      mod4Use.add(mm);
      incomingMods.add(mm);
     }
     else
     {
      mod4MetaUpd.put(mm.meta.getId(), mm);
      mod4DataHld.put(mm.meta.getId(), mm);
      mod4Use.add(mm);
     }
    }
    else // modAux.getStatus() == Status.NEW
    {
     if(dm.getText() == null)
     {
      logRoot.log(Level.ERROR, "Module " + modAux.getOrder() + " is marked for insertion but no data were provided");
      res = false;
      continue;
     }

     if(dm.getId() != null)
     {

      if(exstMod != null)
      {
       logRoot.log(Level.ERROR, "Module " + modAux.getOrder() + " is marked for insertion and has it's own ID (" + dm.getId()
         + ") but this ID it already taken by module of cluster '" + exstMod.getClusterId() + "'");
       res = false;
       continue;
      }
     }
     else
     {
      tmpId[1] = (char) modAux.getOrder();
      dm.setId(new String(tmpId));
     }

     ModMeta mm = new ModMeta();
     mm.meta = dm;
     mm.aux = modAux;

     mm.meta.setDocVersion(mm.meta.getModificationTime());

     mod4Ins.add(mm);
     mod4Use.add(mm);

     incomingMods.add(mm);
    }
   }
  }

  if(origSbm != null && origSbm.getDataModules() != null) // now we are sorting modules from the existing cluster (submission)
  {
   for(DataModuleMeta odm : origSbm.getDataModules())
   {
    String modID = odm.getId();

    ModMeta updMod = mod4DataUpd.get(modID);

    if(updMod == null)
     updMod = mod4MetaUpd.get(modID);

    if(updMod != null)
    {
     if(updMod.meta.getDescription() == null) // if the new module has no description we keep the old one
      updMod.meta.setDescription(odm.getDescription());

     updMod.meta.setSubmissionTime(odm.getSubmissionTime()); // Preserving original submitter and submission time
     updMod.meta.setSubmitter(odm.getSubmitter());

     if(updMod.meta.getText() != null)
      updMod.meta.setDocVersion(updMod.meta.getModificationTime());
     else
      updMod.meta.setDocVersion(odm.getDocVersion());

    }
    else if(!mod4Del.containsKey(modID)) // i.e. module that will be kept untouched
    {
     ModMeta mm = new ModMeta();
     mm.meta = odm;
     mm.origModule = ageStorage.getDataModule( new ModuleKey(id, modID) );

     if(mm.origModule == null)
     {
      logRoot.log(Level.ERROR, "Module '" + modID
        + "' is in the submission db but not in the graph. It means data inconsistency. Please contact system administrator");

      res = false;
      continue;
     }

     mod4Use.add(mm);
     mod4Hld.put(modID, mm);
     mod4DataHld.put(modID, mm);
    }

   }
  }

  if(!res)
   return false;

  Map<String, Integer> globalFileConflicts = null; // = new HashMap<String,Integer>();

  if(files != null && files.size() > 0) // Sorting incoming files
  {
   LogNode fileNode = logRoot.branch("Preparing attachments");

   for(n = 0; n < files.size(); n++)
   {
    FileAttachmentMeta newFileMeta = files.get(n);
    AttachmentAux newAuxInfo = (AttachmentAux) newFileMeta.getAux();

    String cAtId = newAuxInfo.getNewId() != null ? newAuxInfo.getNewId() : newFileMeta.getId(); // atax.getNewId()!=null meant that we want to assign the new ID to some attachment

    if(cAtId == null)
    {
     fileNode.log(Level.ERROR, "File " + newAuxInfo.getOrder() + " has empty ID");
     res = false;
     continue;
    }

    if(newAuxInfo.getStatus() != Status.DELETE)
    {
     for(int k = n + 1; k < files.size(); k++) // All IDs must be unique within the submission
     {
      FileAttachmentMeta ofa = files.get(k);

      AttachmentAux cmpax = (AttachmentAux) ofa.getAux();

      if(cmpax.getStatus() == Status.DELETE)
       continue;

      String cmpAtId = cmpax.getNewId() != null ? cmpax.getNewId() : ofa.getId();

      if(cmpAtId.equals(cAtId))
      {
       fileNode.log(Level.ERROR, "File ID (" + cmpAtId + ") conflict. Files: " + newAuxInfo.getOrder() + " and " + cmpax.getOrder());
       res = false;
       continue;
      }
     }
    }

    FileAttachmentMeta origFileMeta = null;

    if(origSbm != null && origSbm.getAttachments() != null)
    {
     for(FileAttachmentMeta ofa : origSbm.getAttachments())
     {
      if(newFileMeta.getId().equals(ofa.getId()))
      {
       origFileMeta = ofa;
       break;
      }
     }
    }

    if(newAuxInfo.getStatus() == Status.UPDATEORNEW)
    {
     if(origFileMeta != null) // OR ageStorage.getAttachment(fm.getId(),
                              // cstMeta.id) != null
      newAuxInfo.setStatus(Status.UPDATE);
     else
      newAuxInfo.setStatus(Status.NEW);
    }

    if(newAuxInfo.getStatus() == Status.DELETE || newAuxInfo.getStatus() == Status.UPDATE)
    {
     if(origSbm == null) // No original submission. This means a new submission
     {
      fileNode.log(Level.ERROR, "File " + newAuxInfo.getOrder() + " is marked for update/deletion but submission is not in UPDATE mode");
      res = false;
      continue;
     }

     if(origFileMeta == null)
     {
      fileNode.log(Level.ERROR, "File " + newAuxInfo.getOrder() + " is marked for update/deletion but it doesn't exist within the submission");
      res = false;
      continue;
     }

     // fm.setSystemId(origFm.getSystemId());

     if(newFileMeta.getDescription() == null)
      newFileMeta.setDescription(origFileMeta.getDescription());

     newFileMeta.setSubmitter(origFileMeta.getSubmitter());
     newFileMeta.setSubmissionTime(origFileMeta.getSubmissionTime());

     if(newAuxInfo.getStatus() == Status.DELETE)
     {
      if(origFileMeta.isGlobal())
      {
       if(globalFileConflicts == null)
        globalFileConflicts = new HashMap<String, Integer>();

       globalFileConflicts.put(newFileMeta.getId(), -1);
      }

      newFileMeta.setGlobal(origFileMeta.isGlobal());
      att4Del.put(newFileMeta.getId(), newFileMeta);
     }
     else
     // UPDATE
     {
      if(newAuxInfo.getNewId() != null && !newAuxInfo.getNewId().equals(newFileMeta.getId())) // Submitter wants to rename this attachment
      {

       // We have checked that all IDs are unique within this submission let's check conflicts with the global IDs

       if(newFileMeta.isGlobal())
       {
        if(ageStorage.getAttachment(newAuxInfo.getNewId()) != null) // ok, it could be a problem if it not some attachment that we a going to delete
        {
         if(globalFileConflicts == null)
          globalFileConflicts = new HashMap<String, Integer>();

         if(!globalFileConflicts.containsKey(newAuxInfo.getNewId()))
          globalFileConflicts.put(newAuxInfo.getNewId(), newAuxInfo.getOrder());

         // fileNode.log(Level.ERROR, "File " + newAuxInfo.getNewId() +
         // " has global ID but this ID is already taken");
         // res = false;
         // continue;
        }
       }

       // To do renaming we will simulate insertion/deletion

       FileAttachmentMeta nfm = Factory.createFileAttachmentMeta();
       AttachmentAux nax = new AttachmentAux();

       nfm.setAux(nax);
       nax.setStatus(Status.NEW);
       nfm.setId(newAuxInfo.getNewId());
       nax.setOrder(newAuxInfo.getOrder());
       nax.setFile(newAuxInfo.getFile() != null ? newAuxInfo.getFile() : submissionDB.getAttachment(origSbm.getId(), origFileMeta.getId(),
         origFileMeta.getModificationTime()));

       nfm.setSubmitter(origFileMeta.getSubmitter());
       nfm.setModifier(newFileMeta.getModifier());
       nfm.setSubmissionTime(origFileMeta.getSubmissionTime());
       nfm.setModificationTime(newFileMeta.getModificationTime());
       nfm.setDescription(newFileMeta.getDescription());
       nfm.setGlobal(newFileMeta.isGlobal());

       att4Ins.put(nfm.getId(), nfm);
       att4Use.put(nfm.getId(), nfm);

       newAuxInfo.setNewId(null);
       newAuxInfo.setStatus(Status.DELETE);

       newFileMeta.setGlobal(origFileMeta.isGlobal());

       att4Del.put(newFileMeta.getId(), newFileMeta);
      }
      else
      // UPDATE not renaming
      {
       FileMeta fmeta = new FileMeta(); // Our local structure to keep attachment info together

       fmeta.newFile = newFileMeta;
       fmeta.origFile = origFileMeta;
       fmeta.aux = newAuxInfo;

       if(newAuxInfo.getFile() != null)
        att4Upd.put(newFileMeta.getId(), fmeta);
       else
       {
        att4MetaUpd.put(newFileMeta.getId(), fmeta);
        newFileMeta.setFileVersion(origFileMeta.getFileVersion());
       }

       att4Use.put(newFileMeta.getId(), newFileMeta);

       if(newFileMeta.isGlobal() != origFileMeta.isGlobal())
       {
        if(newFileMeta.isGlobal())
         att4L2G.put(newFileMeta.getId(), fmeta);
        else
         att4G2L.put(newFileMeta.getId(), fmeta);
       }

      }

     }
    }
    else if(newAuxInfo.getStatus() == Status.NEW)
    {
     if(newAuxInfo.getFile() == null)
     {
      fileNode.log(Level.ERROR, "File " + newAuxInfo.getOrder() + " is marked as NEW but contains no data");
      res = false;
      continue;
     }

     if(origFileMeta != null)
     {
      fileNode.log(Level.ERROR, "File " + newAuxInfo.getOrder() + " is marked as NEW but file with the same ID already exists");
      res = false;
      continue;
     }

     if(newFileMeta.isGlobal()) // this is a new file with a new global ID. We have to check its uniqueness
     {

      if(ageStorage.getAttachment(newFileMeta.getId()) != null) // ok, it could be a problem if it not attachment that we are going to delete
      {
       if(globalFileConflicts == null)
        globalFileConflicts = new HashMap<String, Integer>();

       if(!globalFileConflicts.containsKey(newFileMeta.getId()))
        globalFileConflicts.put(newFileMeta.getId(), newAuxInfo.getOrder());
      }

      // fm.setSystemId(gid);
     }

     att4Ins.put(newFileMeta.getId(), newFileMeta);
     att4Use.put(newFileMeta.getId(), newFileMeta);

    }

   }

   if(globalFileConflicts != null)
   {
    for(Map.Entry<String, Integer> me : globalFileConflicts.entrySet())
    {
     if(me.getValue() != -1)
     {
      fileNode.log(Level.ERROR, "File " + me.getValue() + " has global ID='" + me.getKey() + "' but this ID is already taken");
      res = false;
     }
    }

    globalFileConflicts = null;
   }

   if(res)
    fileNode.success();

  }

  if(origSbm != null && origSbm.getAttachments() != null)
  {
   for(FileAttachmentMeta fm : origSbm.getAttachments())
   {
    if(!att4Del.containsKey(fm.getId()) && !att4Upd.containsKey(fm.getId()) && !att4MetaUpd.containsKey(fm.getId()))
    {
     att4Hld.put(fm.getDescription(), fm);
     att4Use.put(fm.getId(), fm);
    }
   }
  }
 
  return res;
 }
}
