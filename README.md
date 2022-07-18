


# Pandas-Alert-System-Project

  

1. At start script searches given folder for csv log files and analyze them. Then it starts to monitor folder for new csv flies and when they appear immediately starts analyzing them.

2. Components:

	1. Folder search is based on blob module.

	2. Event for new file is based om watchdog API:

		- event created when new file with csv extension created in folder.

		- then, due to the fact that watchdog doesnt have event handler for file finish copying, we wait for file last modification time stop changing, then analyze it.

		- WORKS from docker on Linux BUT NOT on Windows.

	3. Analysis options done via configparser module:

		- Options are stored in config.txt file.

		- There are 3 default options in DEFAULT section:

			1. groupby - responsible for grouping data via set column "groupby = column_name"

			2. count - min limit for error number "count = 10"

			3. interval - period of time to count errors in format like "interval = HH:MM:SS"

		- Every other section can have unlimited number of columns to filter like "column_name = column value"

		- options from DEFAULT will be added to other sections if there are no such options in them.

		- Options are loaded on script start and can be easily changed or added.

	4. logs analysis is done via pandas:

		- df is filtered for needed values

		- we add 2date columns

		- depending on groupby value we apply window function "rolling" with min max aggregation on dates. This allow us to determine time interval when given number of errors occur.

		- filter all values that do not satisfy our interval condition.

		- write data to file line by line if errors were not used in previous calculation.

5. Script can be executed by setting variables in file .env for input and output log files via docker-compose command.
