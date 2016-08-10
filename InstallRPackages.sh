#!/usr/bin/env bash

#usage - ./InstallRPackages <package1> <package2> <package3> ...
#example - ./InstallRPackages bitops stringr arules

# rxHadoopCopyFromLocal("InstallRPackages.sh", dest = "wasb://rscripts@aapocblob.blob.core.windows.net/")

echo "Sample action script to install R packages..."

if [ -f /usr/bin/R ]
then

	#
	# Example of R package that requires system dependencies
	#

	# echo "Install system dependencies required by rJava..."
	# apt-get -y -f install libpcre3 libpcre3-dev
	# apt-get -y -f install zlib1g-dev

	# echo "Configure R for use with Java..."
	# R CMD javareconf

	#loop through the parameters (i.e. packages to install)
	for i in "$@"; do
		if [ -z $name ]
		then
			name=\'$i\'
		else
			name=$name,\'$i\'
		fi
	done

	echo "Install packages..."
	R --no-save -q -e "install.packages(\"zoo\")"
	R --no-save -q -e "install.packages(\"timeDate\")"
	
	R --no-save -q -e "install.packages(\"digest\")"
	R --no-save -q -e "install.packages(\"gtable\")"
	R --no-save -q -e "install.packages(\"plyr\")"
	R --no-save -q -e "install.packages(\"reshape2\")"
	R --no-save -q -e "install.packages(\"scales\")"
	R --no-save -q -e "install.packages(\"https://cran.r-project.org/src/contrib/ggplot2_2.1.0.tar.gz\", repos = NULL, type=\"source\")"
	
	R --no-save -q -e "install.packages(\"tseries\")"
	R --no-save -q -e "install.packages(\"fracdiff\")"
	R --no-save -q -e "install.packages(\"Rcpp\")"
	R --no-save -q -e "install.packages(\"colorspace\")"
	R --no-save -q -e "install.packages(\"RcppArmadillo\")"
	R --no-save -q -e "install.packages(\"https://cran.r-project.org/src/contrib/forecast_7.1.tar.gz\", repos = NULL, type=\"source\")"
	
	R --no-save -q -e "install.packages(\"https://cran.r-project.org/src/contrib/forecastHybrid_0.1.7.tar.gz\", repos = NULL, type=\"source\")"

else
	echo "R not installed"
	exit 1
fi

echo "Finished"