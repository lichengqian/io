package io

import java.io.FileFilter
import java.io.File
import com.google.common.collect.Maps
import java.util.concurrent.ConcurrentMap
import java.util.List
import java.util.Map
import com.google.common.collect.Lists

interface PollingNotify<T> {
	def T createObject(File file)
    def void onAdd(String path, T obj)
    def void onDelete(String path, T obj)
    def void onChange(String path, T oldValue, T newValue)
}

public final class FileSystemPollingStore<T> {

	public val File location
	public val FileFilter filter
	
    private var boolean pollingActive = true
    private val ConcurrentMap<String, T> compiledObjects = Maps.newConcurrentMap
    private val ConcurrentMap<String, Long> modDateMap   = Maps.newConcurrentMap

	new(File location, FileFilter filter) {
		this.location = location;
		this.filter = filter;
	}
	
    def getObjects() {
    	compiledObjects.values()
    } 

    def private isNewerThanCached(File it) {
        val lastKnownModifiedDate = modDateMap.get(it.canonicalPath)
        return lastKnownModifiedDate == null || it.lastModified() > lastKnownModifiedDate
    }

	def private Map<File, Long> getAllMatchingFiles(File it, Map<File, Long> map) {
	    if (it.isDirectory()) {
	        for (f : it.listFiles()) {
	            switch (f){
	                case f.isDirectory() : f.getAllMatchingFiles(map)
	                case filter.accept(f)  : map.put(f, f.lastModified())
	            }
	        }
	    }
	    return map
	}

    private def Map<File, Long> createMapOnDisk() {
    	return location.getAllMatchingFiles(Maps.newHashMap())
    } 

    private def List<String> getFilesDeletedFromDisk(Map<File, Long> onDiskFiles) {
        val paths = onDiskFiles.keySet().map [it.canonicalPath ] .toList()
        val List<String> deletedPaths = Lists.newArrayList
        for (p : modDateMap.keySet()) {
            if (!paths.contains(p))
                deletedPaths.add(p)
        }
        return deletedPaths
    }

    def refresh(PollingNotify<T> notify) {
        var dirty = false
        val onDiskLastModDateMap = createMapOnDisk()
        val deletedFromDisk = getFilesDeletedFromDisk(onDiskLastModDateMap)
        for (pathToDeleted : deletedFromDisk) {
            val obj = compiledObjects.remove(pathToDeleted)
            modDateMap.remove(pathToDeleted)
            if (obj != null)
                notify.onDelete(pathToDeleted, obj)
            dirty = true
        }
        for (entry : onDiskLastModDateMap.entrySet) {
        	val fileOnDisk = entry.key
        	val t = entry.value
            val pathToUpdate = fileOnDisk.canonicalPath
            if (fileOnDisk.isNewerThanCached()) {
                val obj = notify.createObject(fileOnDisk)
                if (obj != null) {
                    if (compiledObjects.containsKey(pathToUpdate)) {
                        notify.onChange(pathToUpdate, compiledObjects.get(pathToUpdate), obj)
                    } else {
                        notify.onAdd(pathToUpdate, obj)
                    }
                    compiledObjects.put(pathToUpdate, obj)
                    modDateMap.put(pathToUpdate, t)
                    dirty = true
                }
            }
        }
        return dirty
    }

}