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
	#load the data
	for datatype, file in type_to_files.items():
		print("Loading", datatype, "from", file)
		data = file_utils.load_zipped_multi_json(file)
		print("   Loaded", len(data), "objects")

		#find and print a video object with a narrative label and english title
		'''
		if datatype == "videos":
			i = 0
			while data[i]['extension']['detected_language_description'] != "en" or data[i]['extension']['socialsim_information_id'][0] == "" or data[i]['extension']['socialsim_information_id'][0] == '':
				i += 1
			print(data[i])
		'''

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
				print("   No narrative labels for", datatype)
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

		#print results - just the objects without labels for each data type
		if no_label != -1: print("  ", no_label, "objects without narrative")

	#overall counts, across all youtube stuff
	print(len(label_counts), "unique labels")
	for label, count in label_counts.items():
		if label != "": print("  ", label + ":", count)
	print(len(comp_counts), "unique narrative components")
	for comp, count in comp_counts.items():
		if comp != "": print("  ", comp + ":", count)
#end load_domain_data


#----- MAIN EXECUTION -----#

#load the youtube data
print("Loading YouTube data")
load_domain_data(youtube_data_files)

#and the Twitter
print("Loading Twitter data")
#load_domain_data(twitter_data_files)