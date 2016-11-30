#!/usr/bin/python
import glob,os

if __name__ == "__main__":
    if len(sys.argv) != 2:
        l = len(sys.argv)
        print("Check usage in README")
        sys.exit(1)

    pcap_compressed_path =sys.argv[1:]

	os.chdir(pcap_compresses_file_path)
	for file in glob.glob("*.gz"):
		str0 = "gunzip "+ file
		os.system(str0)

	for filename in glob.glob("*.pcap"):
		str = "yaf --silk --flow-stats --in " + filename + " --out " + filename +".yaf"
		str2 = "rwipfix2silk --silk-output=" + filename + ".rw "+ filename+".yaf"
		str3 = "rwcut --output-path="+ filename +".csv --delimited=, --timestamp-format=iso "+ filename +".rw"
		os.system(str)
		os.system(str2)
		os.system(str3)

	for rwfilename in glob.glob("*.rw"):
		os.system("rm " +rwfilename)

	for yaffilename in glob.glob("*.yaf"):
		os.system("rm " +yaffilename)

	os.system("mkdir csv")
	for csvname in glob.glob("*.csv"):
		os.system("mv " + csvname + " csv/"+ csvname)
