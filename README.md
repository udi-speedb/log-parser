## Speedb-Log-Parser: A tool for parsing RocksDB and Speedb Log Files

<!-- ABOUT THE PROJECT -->
## About The Project
Speedb's Log Parser is a tool that may be used to parse and process 
Speedb and RocksDB log files.

The tool extracts useful information from these logs and aids users in 
gaining insights about their systems with ease. It is expected to be valuable tool
for novices and experts alike.




<!-- GETTING STARTED -->
## Getting Started
The tool runs on a single log file and generates one or more outputs:
1. A short summary printed to the console
2. A detailed summary in a JSON format
3. The detailed JSON file printer to the console. This may be filtered 
   using tools such as (e.g., JQ)
4. A CSV file with the statistics counters (if available)
5. A CSV(s) file with the statistics histograms counters (if available). 
   There are 2 such files generated, one aimed at humans (easier to read 
   for humans), and the other aimed at automated tools.
6. A CSV file with the compaction statistics
7. A log file of the tool's run (used for debugging purposes)

By default, a short console summary will be displayed. Users may request the 
tool to also generate the JSON detailed summary, and the detailed console 
summary.

The outputs are placed in a separate folder (the folder is DELETED if 
already exists). By default, "output_files" will be the name of that folder 
but the user may override that.

Running the tool without any parameters will allow users to view the 
possible flags the tool supports:
   ```sh
   pytho3 log_parser.py
   ```

And also get detailed help information:
   ```sh
   pytho3 log_parser.py -h
   ```


### Prerequisites

Python version 3 (to run the parser tool) [TBD - Which eact minor Python 3 
version?]

### Installation

1. Clone the repo
   ```sh
   git clone git@github.com:speedb-io/log-parser.git
   ```

2. pytest installation (only if you wish / need to run the pytest unit 
   tests under the test folder)
   ```sh
   pip install pytest
   ```
3. flake8 (only if you modify any files or add new ones)
   ```sh
   pip install flake8
   ```

### Testing
The tool comes with a set of pytest unit tests.
To run the unit tests:
1. Install pytest (see Installaion section for details)
2. Go to the test folder
   ```sh
   cd test
   ```

3. Run the tests
   ```sh
   pytest
   ```
   

<!-- USAGE EXAMPLES -->
## Usage

The repo contains a sample file that may be used to run the tool:
   ```sh
   pytho3 test/input_files/LOG_speedb
   ```


<!-- ROADMAP -->
## Roadmap

TBD


See the [open issues](TBD) for a full list of proposed features (and known issues).



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Add / Modify / Update unit tests to the test folder as applicable
4. Verify that all existing and new unit tests pass:
   ```sh
   cd test
   pytest
   ```
5. Verify that all the parser's python scripts and test scripts cleanly 
   pass the flake8 verification tool:
   ```sh
   flake8 *.py
   flake8 test/*.py
   ```
6. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
7. Push to the Branch (`git push origin feature/AmazingFeature`)
8. Open a Pull Request


<!-- LICENSE -->
## License

Distributed under the Apache V2 License. See `LICENSE.txt` for more information.



<!-- CONTACT -->
## Contact

TBD
