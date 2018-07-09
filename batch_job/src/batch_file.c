/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_file.h"
#include "sha1.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "path.h"

/**
 * Create batch_file from outer_name and inner_name.
 * Outer/DAG name indicates the name that will be on the host/submission side.
 *  This is equivalent to the filename in Makeflow.
 * Inner/task name indicates the name that will be used for execution.
 *  IF no inner_name is given, or the specified batch_queue does not support
 *  remote renaming the outer_name will be used.
 **/
struct batch_file *batch_file_create(struct batch_queue *queue, const char * outer_name, const char * inner_name)
{
	struct batch_file *f = calloc(1,sizeof(*f));
    f->outer_name = xxstrdup(outer_name);

	if(batch_queue_supports_feature(queue, "remote_rename") && inner_name){
		f->inner_name = xxstrdup(inner_name);
	} else {
		f->inner_name = xxstrdup(outer_name);
	}

    return f;
}

/**
 * Delete batch_file, including freeing outer_name and inner_name/
 **/
void batch_file_delete(struct batch_file *f)
{
	if(!f)
		return;

	free(f->outer_name);
	free(f->inner_name);

	free(f);
}

/**
 * Given a file, return the string that identifies it appropriately
 * for the given batch system, combining the local and remote name
 * and making substitutions according to the node.
 **/
char * batch_file_to_string(struct batch_queue *queue, struct batch_file *f )
{
    if(batch_queue_supports_feature(queue,"remote_rename")) {
            return string_format("%s=%s", f->outer_name, f->inner_name);
    } else {
            return string_format("%s", f->outer_name);
    }
}

/**
 * Given a list of files, add the files to the given string.
 * Returns the original string, realloced if necessary
 **/
char * batch_files_to_string(struct batch_queue *queue, struct list *files )
{
    struct batch_file *file;

    char * file_str = strdup("");

	char * separator = "";

    if(!files) return file_str;

    list_first_item(files);
    while((file=list_next_item(files))) {
		/* Only add separator if past first item. */
		file_str = string_combine(file_str,separator);

        char *f = batch_file_to_string(queue, file);
        file_str = string_combine(file_str,f);
	
		/* This could be set using batch_queue feature or option 
		 * to allow for batch system specific separators. */
		separator = ",";

        free(f);
    }

    return file_str;
}

int batch_file_outer_compare(const void *file1, const void *file2) {
	struct batch_file **f1 = (void *)file1;
	struct batch_file **f2 = (void *)file2;

	return strcmp((*f1)->outer_name, (*f2)->outer_name);
}

/* Return the content based ID for a file.
 * generates the checksum of a file's contents if does not exist */
char * batch_file_generate_id(struct batch_file *f) {
	if(!f->hash){
		unsigned char *hash = xxcalloc(1, sizeof(char *)*SHA1_DIGEST_LENGTH);
		sha1_file(f->outer_name, hash);
		f->hash = xxstrdup(sha1_string(hash));
		free(hash);
	}	
	return xxstrdup(f->hash);
}

/* Return if the name of a file is a directory */
int is_dir(char *file_name){
	if(path_has_doubledots(file_name) || file_name[0] == '.'){
		return -1;
	}
	else{
		//Grabbed from https://stackoverflow.com/questions/4553012/checking-if-a-file-is-a-directory-or-just-a-file
		DIR* directory = opendir(file_name);

		if(directory != NULL){
			closedir(directory);
			debug(D_MAKEFLOW_HOOK, "%s is a DIRECTORY",file_name);
			return 0;
		}

		if(errno == ENOTDIR){
			return 1;
		}

		return -1;
	}
}

/* Return the content based ID for a directory.
 * generates the checksum for the directories contents if does not exist */
char *  batch_file_generate_id_dir(char *file_name){
        debug(D_MAKEFLOW,"THIS CODE IS RUNNING");
        char *hash_sum = "";
        struct dirent *dp;
        DIR *dfd = opendir(file_name);
        while((dp = readdir(dfd)) != NULL){
		if(!(strcmp(dp->d_name,".") == 0) && !(strcmp(dp->d_name,"..") == 0)){
			char *file_path = string_format("%s/%s",file_name,dp->d_name);
			if(is_dir(file_path) == 0){
				hash_sum = string_format("%s%s",hash_sum,batch_file_generate_id_dir(file_path));
			}
			else{
				unsigned char *hash = xxcalloc(1, sizeof(char *)*SHA1_DIGEST_LENGTH);
				debug(D_MAKEFLOW, "THIS IS THE DP_DNAME: %s", file_path);
				sha1_file(file_path, hash);
				debug(D_MAKEFLOW, "THIS IS THE HASH OF DP_DNAME: %s",hash);
				hash_sum = string_format("%s%s",hash_sum,xxstrdup(sha1_string(hash)));
				debug(D_MAKEFLOW, "THIS IS THE HASH SUM: %s",hash_sum);
				free(hash);
			}
		}
        }
        closedir(dfd);
	debug(D_MAKEFLOW,"THIS IS THE FINAL HASH SUM: %s",hash_sum);
        return xxstrdup(hash_sum);

}
