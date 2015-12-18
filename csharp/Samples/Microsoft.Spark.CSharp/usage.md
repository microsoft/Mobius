SparkCLRSamples.exe supports following options:
   
   [--temp | spark.local.dir] <TEMP_DIR>                 TEMP_DIR is the directory used as "scratch" space in Spark, including map output files and RDDs that get stored on disk. 
                                                         See http://spark.apache.org/docs/latest/configuration.html for details.
   
   [--data | sparkclr.sampledata.loc] <SAMPLE_DATA_DIR>  SAMPLE_DATA_DIR is the directory where data files used by samples reside. 
   
   [--torun | sparkclr.samples.torun] <SAMPLE_LIST>      SAMPLE_LIST specifies a list of samples to run, samples in list are delimited by comma. 
                                                         Case-insensitive command line wild card matching by default. Or, use "/" (forward slash) to enclose regular expression. 
   
   [--cat | sparkclr.samples.category] <SAMPLE_CATEGORY> SAMPLE_CATEGORY can be "all", "default", "experimental" or any new categories. 
                                                         Case-insensitive command line wild card matching by default. Or, use "/" (forward slash) to enclose regular expression. 
   
   [--validate | sparkclr.enablevalidation]              Enables validation of results produced in each sample. 
   
   [--dryrun | sparkclr.dryrun]                          Dry-run mode. Just lists the samples that will be executed with given parameters without running them
   
   [--help | -h | -?]                                    Display usage. 
   
   
 Usage examples:  
   
   Example 1 - run default samples:
   
     SparkCLRSamples.exe --temp C:\gitsrc\SparkCLR\run\Temp --data C:\gitsrc\SparkCLR\run\data 
   
   Example 2 - dryrun default samples:
   
     SparkCLRSamples.exe --dryrun 
   
   Example 3 - dryrun all samples:
   
     SparkCLRSamples.exe --dryrun --cat all 
   
   Example 4 - dryrun PiSample (commandline wildcard matching, case-insensitive):
   
     SparkCLRSamples.exe --dryrun --torun pi*
   
   Example 5 - dryrun all DF* samples (commandline wildcard matching, case-insensitive):
   
     SparkCLRSamples.exe --dryrun --cat a* --torun DF*
   
   Example 6 - dryrun all RD* samples (regular expression):
   
     SparkCLRSamples.exe --dryrun --cat a* --torun /\bRD.*Sample.*\b/
   
   Example 7 - dryrun specific samples (case insensitive): 
   
     SparkCLRSamples.exe --dryrun --torun "DFShowSchemaSample,DFHeadSample"
   
