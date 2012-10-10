package uk.ac.ebi.age.transaction;

public interface ReadLock extends Lock
{
 boolean isActive();
}
