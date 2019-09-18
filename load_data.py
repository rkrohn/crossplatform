#load raw Twitter + YouTube White Helmets data

import file_utils

from collections import defaultdict

#file locations

#youtube originally from /data/socsim/2019DecCP/White_Helmet/YouTube/Tng_an_WH_Youtube.tar, but these are the unpacked files
youtube_data_files = {
					"captions": "./data/YouTube/Tng_an_Captions.json.gz", 
					"channels": "./data/YouTube/Tng_an_Channels.json.gz", 
					"comments": "./data/YouTube/Tng_an_Comments.json.gz", 
					"comment_replies": "./data/YouTube/Tng_an_CommentReplies.json.gz", 
					"videos": "./data/YouTube/Tng_an_Videos.json.gz"
					}
#twitter data directly from source
twitter_data_files = {
					"tweets": "/data/socsim/2019DecCP/White_Helmet/Twitter/Tng_an_WH_Twitter_v2.json.gz",
					"retweet_chain": "/data/socsim/2019DecCP/White_Helmet/Twitter/Tng_an_Retweet_Chain_WH.json.gz",
					"botometer_en": "/data/socsim/2019DecCP/White_Helmet/Twitter/Tng_an_en_Twitter_WH_botometer_results.json.gz",
					"botometer_ar": "/data/socsim/2019DecCP/White_Helmet/Twitter/Tng_an_ar_Twitter_WH_botometer_results.json.gz"
}

#file containing mapping from search term -> narrative label component
search_term_mapping_file = "./data/search_term_mapping.csv"


#given a dictionary of data type -> filename, load domain data
def load_domain_data(type_to_files):
	#load data into dictionary, where key is datatype as given in filename dict
	data_dict = {}

	#load the data
	for datatype, file in type_to_files.items():
		print("Loading", datatype, "from", file)
		data_dict[datatype] = file_utils.load_zipped_multi_json(file)
		print("   Loaded", len(data_dict[datatype]), "objects")		

	#return data in dictionary, with same format as filename input
	return data_dict
#end load_domain_data


#given a loaded data collection (of a single type), do some narrative label analysis
def narrative_analysis(data, datatype=""):
	#narrative labels live in the ['extension']['socialsim_information_id'] field
	#list of strings, if no label contains ''

	print(datatype)
	print("  ", len(data), "objects")

	#how many objects don't have a narrative assigned?
	no_label = 0
	#how many unique narrative labels?
	label_counts = defaultdict(int)
	#how many unique label components? (ie, many components to a single narrative label)
	comp_counts = defaultdict(int)
	#loop all data objects
	for item in data:
		#skip if these items don't have extensions
		if 'extension' not in item:
			no_label = -1
			break
		#count items with no narrative label
		if item['extension']['socialsim_information_id'][0] == "" or item['extension']['socialsim_information_id'][0] == '':
			no_label += 1
		#add to counter for each narrative label
		for label in item['extension']['socialsim_information_id']:
			label_counts[label] += 1
			#and break that down into individual components
			for comp in label.split('-'):
				comp_counts[comp] += 1

	#no narrative labels for these objects - return
	if no_label == -1:
		print("   No narrative labels for", datatype)
		return {"unlabeled_count": len(data), "label_freq": {}, "comp_freq": {}}

	#remove empty label from frequency counts
	del label_counts['']
	del comp_counts['']

	#print results

	#count of objects with label
	print("   %d objects with narrative (%.3f)" % (len(data)-no_label, (len(data)-no_label)/len(data)))

	#overall counts, across all objects of this type
	print("  ", len(label_counts), "unique labels")
	'''
	for label, count in label_counts.items():
		if label != "": print("  ", label + ":", count)
	'''
	print("  ", len(comp_counts), "unique narrative components")
	'''
	for comp, count in comp_counts.items():
		if comp != "": print("  ", comp + ":", count)
	'''

	#return counts and such
	return {"unlabeled_count": no_label, "label_freq": label_counts, "comp_freq": comp_counts}
#end narrative_analysis


#perform narrative label analysis on all data for an entire platform (across all data types)
def platform_narrative_analysis(data, platform=""):
	narr_res = {}		#counts for each datatype
	#how many unique labels/components, across all data objects?
	uniq_labels = set()
	uniq_comps = set()

	print("\n%snarrative analysis" % (platform+" " if platform != "" else ""))
	for datatype, data in data.items():
		narr_res[datatype] = narrative_analysis(data, datatype)
		uniq_labels = uniq_labels.union(set(narr_res[datatype]['label_freq'].keys()))
		uniq_comps = uniq_comps.union(set(narr_res[datatype]['comp_freq'].keys()))
	print(len(uniq_labels), "unique labels and", len(uniq_comps), "unique components overall")

	#return type-specific frequencies, and sets of unique labels and components
	return narr_res, uniq_labels, uniq_comps
#end platform_narrative_analysis


#search data for narrative keywords
#only search the list of fields given as argument (for flexibility)
#each field will itself be a list, where subsequent entries are nested below the previous
#skip any fields that don't exist (because some are optional, ugh)
def search_narrative_keywords(data, fields, keywords_dict):
	#store item narratives in nested dict
	#id_h -> given-> given narrative label
	#id_h -> inferred -> inferred narrative label, based on keywords
	item_narratives = {}	 

	#loop each object
	for item in data:
		#pull given narrative label
		item_narratives[item['id_h']] = {'inferred': []}
		item_narratives[item['id_h']]['given'] = item['extension']['socialsim_information_id'] if item['extension']['socialsim_information_id'][0] != '' else []

		#search each field
		for field in fields:
			#grab desired text field
			text = item
			for subfield in field:
				if subfield in text:
					text = text[subfield]
				else:
					text = None
					break

			#skip if bad text (ie, skipping field)
			if text is None: break

			#if text is a list (tags), make into a string
			if isinstance(text, list):
				text = ' '.join(text)

			#convert text to all lowercase
			text = text.lower()

			#print(field[-1], "text:", text)

			#look for all keywords in this text
			for keyword, label in keywords_dict.items():
				if keyword in text and label not in item_narratives[item['id_h']]['inferred']:
					item_narratives[item['id_h']]['inferred'].append(label)
					#print("\0u' %s ' in %s -> %s" % (keyword, field[-1], label))

		#print("given", item_narratives[item['id_h']]['given'])
		#print("inferred", item_narratives[item['id_h']]['inferred'])

	return item_narratives		#return dictionary
#end search_narrative_keywords


#given assigned narrative labels, break into set of components present in any narrative label
def narratives_to_comp(all_narrative_dict):
	#loop all objects
	for obj_id, narrative_dict in all_narrative_dict.items():
		#new dict entry for list of given components
		narrative_dict['given_comp'] = []
		#loop each narrative label
		for label in narrative_dict['given']:
			#and split
			for comp in label.split('-'):
				if comp not in narrative_dict['given_comp']:
					narrative_dict['given_comp'].append(comp)
	#return updated dict
	return all_narrative_dict
#end narratives_to_comp


#write narrative frequencies to file
def save_narrative_freq(narr_dict, labels, components, filename):
	#first column will be list of labels/components, following will be frequencies for each datatype
	#two separate files for labels and components (argument is basefilename)

	#build lists for labels file
	col_headers = ['narrative_label']
	all_cols = [list(labels)]		#start list of all columns, where first col is list of narr labels
	#new column list for each data type
	for datatype, freq_dict in narr_dict.items():
		new_col = []
		for label in all_cols[0]:
			new_col.append(freq_dict['label_freq'][label] if label in freq_dict['label_freq'] else 0)
		col_headers.append(datatype+"_freq")
		all_cols.append(new_col)
	#save labels
	file_utils.lists_to_csv(all_cols, col_headers, filename+"_narrative_labels.csv")

	#and the same for narrative components
	col_headers = ['narrative_components']
	all_cols = [list(components)]		#start list of all columns, where first col is list of narr labels
	#new column list for each data type
	for datatype, freq_dict in narr_dict.items():
		new_col = []
		for label in all_cols[0]:
			new_col.append(freq_dict['comp_freq'][label] if label in freq_dict['comp_freq'] else 0)
		col_headers.append(datatype+"_freq")
		all_cols.append(new_col)
	#save labels
	file_utils.lists_to_csv(all_cols, col_headers, filename+"_narrative_components.csv")

	print("\nFrequency data saved to %s_narrative_labels.csv and %s_narrative_components.csv" % (filename, filename))
#end save_narrative_freq




#----- MAIN EXECUTION -----#

print("")

#load search term -> narrative component mapping
search_term_dict = file_utils.read_csv_dict(search_term_mapping_file)

print(len(search_term_dict), "search keywords")
#print(list(search_term_dict.keys()))


#load the youtube data
print("\nLoading YouTube data")
youtube_data = load_domain_data(youtube_data_files)

#and the Twitter
print("\nLoading Twitter data")
twitter_data = load_domain_data(twitter_data_files)

#narrative analysis
#YouTube
youtube_narr_res, youtube_labels, youtube_comps = platform_narrative_analysis(youtube_data, "YouTube")
#Twitter
#narrative analysis
twitter_narr_res, twitter_labels, twitter_comps = platform_narrative_analysis(twitter_data, "Twitter")

#combine into single list of labels and components
all_labels = youtube_labels.union(twitter_labels)
all_comps = youtube_comps.union(twitter_comps)
print("\n%d labels, %d components across both platforms" % (len(all_labels), len(all_comps)))

#save frequencies to files (4 separate for now, can't be bothered to combine further)
save_narrative_freq(youtube_narr_res, all_labels, all_comps, "results/youtube_freq")
save_narrative_freq(twitter_narr_res, all_labels, all_comps, "results/twitter_freq")



exit(0)

#does the video title/description/tags lead to the same narrative labels?
#videos_narrative_dict = search_narrative_keywords(youtube_data['videos'], [["snippet","title_m"], ["snippet","description_m"], ["snippet","tags"]], search_term_dict)
'''
print("\nvideos with given narrative, vs inferred")
for video_id, narrative_dict in videos_narrative_dict.items():
	if len(narrative_dict['given']) > 0:
		print(video_id, narrative_dict['given'], narrative_dict['inferred'])
'''

#convert given/assigned narrative labels to list of components - for easier comparison against inferred
#videos_narrative_dict = narratives_to_comp(videos_narrative_dict)
'''
print("\nvideos with given narrative components, vs inferred")
for video_id, narrative_dict in videos_narrative_dict.items():
	if len(narrative_dict['given_comp']) > 0:
		print(video_id, narrative_dict['given_comp'], narrative_dict['inferred'])
'''

'''
#which given components did we not find?
print("\nvideos and narrative components not found via keywords")
missing_comp_count = 0	#count of videos with missing components
for video_id, narrative_dict in videos_narrative_dict.items():
	vid_count = 0
	for comp in narrative_dict['given_comp']:
		if comp not in narrative_dict['inferred']:
			if vid_count == 0: print(video_id, ":", end=" ")
			print(comp, end=" ")
			vid_count += 1
	if vid_count != 0: 
		print("")
		missing_comp_count += 1
print(missing_comp_count, "videos missing given components (of", len(youtube_data['videos']) - youtube_narr_res['videos']['unlabeled_count'], "labelled)")

#single video - how are we missing keywords?

print("")
vid_id = "0FPAb4yCjI1Jfyzy_YyYiA"
for video in youtube_data['videos']:
	if video['id_h'] == vid_id:
		print("title:", video['snippet']['title_m'])
		print("description:", video['snippet']['description_m'])
print("")
print(videos_narrative_dict[vid_id])

exit(0)

#are there duplicate videos in the dataset?
print("")
print(len(youtube_data['videos']), "videos in list")
print(len(set(video['id_h'] for video in youtube_data['videos'])), "videos in set")

#who are the duplicates, and are they the same?
#list of ids in list
id_list = [video['id_h'] for video in youtube_data['videos']]
#duplicate ids
id_duplicates = set([x for x in id_list if id_list.count(x) > 1])
print(len(id_duplicates), "duplicates:", id_duplicates)

exit(0)
'''


