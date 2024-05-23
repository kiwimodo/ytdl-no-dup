import re
import os
import yt_dlp
from dateutil import parser
from yt_dlp.compat import shutil

# -------------------------------- Globals --------------------------------
log_file = open("log.txt", "w")

# INSERT LINKS HERE
todo_urls = [
	# "<EXAMPLE LINK TO VIDEO>"
	# "<EXAMPLE LINK TO PLAYLIST>"
	# "<EXAMPLE LINK TO USER PAGE>"
]

parent_table = {}	# Tree of ID's that correspond to URLs
title_dict = {}		# Mapping of IDs to titles
video_urls = []		# Queue of URLs that still need to be downloaded
work_list = []		# Queue of URLs that need to be processed
duplicates = {}		# ID's mapped to lists of parents of which the ID has not been linked
move_paths = {}		# Final paths and titles for duplicate reports

# yt-dlp options for generating URL tree without downloading anything
ydl_flat_opts = {
	'extract_flat': 'in_playlist',
	'extractor_args': {'youtube': { 'player_client' : ['android, web'],
									'player_skip': ['webpage', 'configs', 'js'],
									'skip': ['hls', 'dash', 'translated_subs']}},
	'fragment_retries': 10,
	'ignoreerrors': 'only_download',
	'retries': 10,
	'simulate': True,
	'skip_download': True
}

# yt-dlp options for downloading and remuxing into MKVs.
ydl_dl_opts = {
	'allow_multiple_audio_streams': True,
	'allow_multiple_video_streams': True,
	'extract_flat': 'discard_in_playlist',
	'final_ext': 'mkv',
	'fragment_retries': 10,
	'ignoreerrors': 'only_download',
	'postprocessors': [
		{'key': 'FFmpegVideoRemuxer', 'preferedformat': 'mkv'},
		{'already_have_subtitle': False, 'key': 'FFmpegEmbedSubtitle'},
		{'key': 'FFmpegConcat', 'only_multi_video': True, 'when': 'playlist'}
	],
	'retries': 10,
	'writeautomaticsub': True
}

# -------------------------------- Gross Logging Stuff --------------------------------
levels = ["DEBUG", "INFO", "WARN", "ERROR"]

# Raw log printing
def _log(msg):
	print(msg)
	if log_file != None:
		log_file.write(msg + "\n")

# Formated log printing
def _log_fmt(lvl, msg):
	j_fmt = " ".rjust(14,'-') + levels[lvl] + ": "
	_log(j_fmt + msg)


# Prints the important details of an 'info' object extracted by yt-dlp
def _log_info_data(info, url, print_parent_chain):
	info_url = None
	info_type = None
	ntab = "\n\t\t| "

	s = [	"\t\t| " + "ID:".ljust(14, ' ') + info["id"],
			"Title:".ljust(14, ' ') + info["title"],
			"Provided URL:".ljust(14, ' ') + str(url) ]

	if "url" in info:
		info_url = info["url"]
	if "_type" in info:
		info_type = info["_type"]
	
	s.append("Info URL:".ljust(14, ' ') + str(info_url))
	s.append("Info Type:".ljust(14, ' ') + str(info_type))

	parent_id = parent_table[info["id"]]
	if print_parent_chain == False:
		s.append("Parent Id:".ljust(14, ' ') + str(parent_id))
		_log(ntab.join(s))
		return

	s.append("Parent ID chain:")
	while parent_id != None:
		s.append("\tId: [" + parent_id + "] Title: " + title_dict[parent_id])
		parent_id = parent_table[parent_id]

	_log(ntab.join(s))

# -------------------------------- Classes --------------------------------

# Post processor class for creating directory structure and moving files. TODO: Better way?
class MoveRenamePP(yt_dlp.postprocessor.PostProcessor):

	# Function for generating an output file path w/out file extension
	# and with a sort-friendly prepended date, if a date is in the title.
	def _gen_output_path(self, id):
		title = title_dict[id]
		_log_fmt(0, "Generating output path from title: " + title)

		try:
			# Remove, reformat, and prepend date if it exists
			_log_fmt(0, "Searching for the first occurance of a date in title.")
			title_toks = re.split(r'(\d+/\d+/\d+)', title, 1)
			
			if len(title_toks) > 1:
				_log_fmt(0, "Date found: " + title_toks[1])
				date_str = str(parser.parse(title_toks[1]))[:10]
				title = date_str + " - " + title_toks[0].strip()
				rside = title_toks[2].strip()
				
				if rside != '':
					title += " - " + rside

		except parser.ParserError as err:
			pass # There was no date
		
		_log_fmt(0, "New title, pre-sanitize: " + title)
		re.sub(r'[^\w\d-]', '_', title) # Sanitize
		_log_fmt(0, "New sanitized title: " + title)
		move_paths[id] = title # Store this title for dup report

		path = title
		parent_id = parent_table[id]

		# Traverse and get parent entry titles for directory names
		while parent_id != None:
			dir_name = title_dict[parent_id]
			re.sub(r'[^\w\d-]', '_', dir_name) # Sanitize
			path = os.path.join(dir_name, path)
			move_paths[parent_id] = path # Store this parent's path for dup report
			parent_id = parent_table[parent_id]
		
		path = os.path.join(os.getcwd(), path)
		_log_fmt(0, "New path: " + path)
		return path


	# Post-processesor run function, called by yt-dlp
	def run(self, info):
		oldpath = info['filepath']

		# Create a new path from title and old path
		newpath = self._gen_output_path(info["id"])
		ext = os.path.splitext(oldpath)[1]
		newpath += ext

		# Perform file operations
		_log_fmt(1, "Moving " + oldpath + " to " + newpath)
		yt_dlp.utils.make_dir(newpath, yt_dlp.utils.PostProcessingError)
		shutil.move(oldpath, newpath)
		return [], info

# -------------------------------- Top Level Functions --------------------------------

# Checks to see if an entry has already been processed
def has_been_handled(entry, curr_parent_id):
	if entry["id"] not in parent_table:
		return False
	
	# Gross logging
	_log_fmt(1, "Duplicate!")
	_log_info_data(entry, None, True)

	# Track duplicate instances
	if entry["id"] not in duplicates:
		duplicates[entry["id"]] = []

	duplicates[entry["id"]].append(curr_parent_id)
	return True

# Process extracted information from a URL
def process_info(ydl, info, url):
	if info["id"] not in title_dict:
		title_dict[info["id"]] = info["title"]

	_log_fmt(1, "Processing info:")
	_log_info_data(info, url, False)

	# If the entry is a video, add it to the download list for later
	if "_type" not in info or info["_type"] == "video":
		_log_fmt(1, "Video found. Adding to download list.")
		video_urls.append(url)

	# If the entry is a playlist, iterate through the child entries,
	# linking them to this parent and adding them to the work queue
	elif info["_type"] == "playlist":
		_log_fmt(1, "Playlist found. Processing entries")

		for e in info["entries"]:
			if not has_been_handled(e, info["id"]): # Avoid duplicates linking duplicates
				parent_table[e["id"]] = info["id"]
				_log_fmt(1, "Entry url found. Adding URL to work queue.")
				_log_info_data(e, None, False)
				work_list.append(e["url"])
	

# Repeatedly poll the work queue to get URLs to extract information from
def process_work_list(ydl):
	while len(work_list) > 0:
		url = work_list.pop(0)
		_log_fmt(1, "Getting info for work URL: " + url)
		info = ydl.extract_info(url, download=False)
		if info != None:
			process_info(ydl, info, url)


# Empty the download list and download from each contained URL
def do_downloads(ydl):
	while len(video_urls) > 0:
		url = video_urls.pop(0)
		_log_fmt(1, "Downloading from URL: " + url)
		ydl.download(url)


# Outputs a file containing the location of items that may also belong in other directories
def gen_duplicate_report():
	with open("dup_list.txt", "w") as dups_file:
		for id, dup_list in duplicates.items():
			dups_file.write("ID: [" + id + "] Title: " + move_paths[id] + "\n")
			dups_file.write("Located in directory: " + move_paths[parent_table[id]] + "\n")
			dups_file.write("May also belong in:\n")

			for dup_id in dup_list:
				if dup_id in move_paths:
					dir = move_paths[dup_id]
				else:
					dir = dup_id + "(No Directory Created)"

				dups_file.write("\t" + dir + "\n")
			dups_file.write("\n")
		dups_file.close()


# Main :)
def main():
	_log_fmt(1, "Starting")

	# Perform all operations for each root URL
	for url in todo_urls:
		ydl = yt_dlp.YoutubeDL(ydl_flat_opts)
		_log_fmt(1, "Processing root URL: " + url)

		# Grab info for root URL, skipping if yt-dlp fails
		info = ydl.extract_info(url, download=False)
		if info == None:
			continue

		parent_table[info["id"]] = None
		process_info(ydl, info, url)

		# Start work loop
		_log_fmt(1, "Running work loop for root URL" + url)
		process_work_list(ydl)

		# Download with specified post-processor
		ydl = yt_dlp.YoutubeDL(ydl_dl_opts)
		ydl.add_post_processor(MoveRenamePP(), when='after_move')
		do_downloads(ydl)

	# Generate report for entries that appear in multiple playlists.
	gen_duplicate_report()

	_log_fmt(1, "Finished")
	log_file.close()



if __name__ == "__main__":
	main()