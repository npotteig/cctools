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
#include "hash_table.h"


struct hash_table *check_sums = NULL;

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
	if(check_sums == NULL){
		check_sums = hash_table_create(0,0);
        }
	char *check_sum_value = hash_table_lookup(check_sums, f->outer_name);
	if(check_sum_value == NULL){
		unsigned char *hash = xxcalloc(1, sizeof(char *)*SHA1_DIGEST_LENGTH);
		int success = sha1_file(f->outer_name, hash);
		if(success == 0){
			debug(D_MAKEFLOW, "Unable to checksum this file: %s", f->outer_name);
		}
		f->hash = xxstrdup(sha1_string(hash));
		hash_table_insert(check_sums, f->outer_name, xxstrdup(sha1_string(hash)));
		free(hash);
		return xxstrdup(f->hash);
	}
	debug(D_MAKEFLOW,"CHECKSUM HAS ALREADY BEEN COMPUTED FOR %s",f->outer_name);
	return xxstrdup(check_sum_value);
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
 * generates the checksum for the directories contents if does not exist
 * 		*NEED TO ACCOUNT FOR SYMLINKS LATER*  */
char *  batch_file_generate_id_dir(char *file_name){
	if(check_sums == NULL){
		check_sums = hash_table_create(0,0);
	}	
	char *check_sum_value = hash_table_lookup(check_sums, file_name);
	if(check_sum_value == NULL){
		char *hash_sum = "";
		struct dirent **dp;
		int num;
		// Scans directory and sorts in reverse order
		num = scandir(file_name, &dp, NULL, alphasort);
		if(num < 0){
			debug(D_MAKEFLOW,"Unable to scan %s", file_name);
			return "";
		}
		else{
			while(num--){
				if(!(strcmp(dp[num]->d_name,".") == 0) && !(strcmp(dp[num]->d_name,"..") == 0)){
					char *file_path = string_format("%s/%s",file_name,dp[num]->d_name);
					if(is_dir(file_path) == 0){
						hash_sum = string_format("%s%s",hash_sum,batch_file_generate_id_dir(file_path));
					}
					else{
						unsigned char *hash = xxcalloc(1, sizeof(char *)*SHA1_DIGEST_LENGTH);
						debug(D_MAKEFLOW, "THIS IS THE DP_DNAME: %s", file_path);
						int success = sha1_file(file_path, hash);
						if(success == 0){
							debug(D_MAKEFLOW, "Unable to checksum this file: %s", file_path);
						}
						hash_sum = string_format("%s%s",hash_sum,sha1_string(hash));
						debug(D_MAKEFLOW, "THIS IS THE HASH SUM: %s",hash_sum);
						free(hash);
					}
				}
			}
			free(dp);
			unsigned char hash[SHA1_DIGEST_LENGTH];
			sha1_buffer(hash_sum, strlen(hash_sum), hash);
			hash_table_insert(check_sums, file_name, xxstrdup(sha1_string(hash)));
			debug(D_MAKEFLOW,"THIS IS THE FINAL HASH SUM: %s",sha1_string(hash));
			return xxstrdup(sha1_string(hash));
		}
	}
	debug(D_MAKEFLOW,"CHECKSUM HAS ALREADY BEEN COMPUTED FOR %s",file_name);	
	return xxstrdup(check_sum_value);
}
