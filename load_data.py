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

#----- MAIN EXECUTION -----#

#load the youtube data
print("Loading YouTube data")
youtube_data = load_domain_data(youtube_data_files)

#narrative analysis
youtube_narr_res, youtube_labels, youtube_comps = platform_narrative_analysis(youtube_data, "YouTube")
print(sorted(youtube_comps))
print(sorted(youtube_labels))

#exit(0)

#and the Twitter
print("\nLoading Twitter data")
twitter_data = load_domain_data(twitter_data_files)

#narrative analysis
twitter_narr_res, twitter_labels, twitter_comps = platform_narrative_analysis(twitter_data, "Twitter")
print(sorted(twitter_comps))
print(sorted(twitter_labels))

