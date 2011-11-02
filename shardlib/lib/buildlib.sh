JUNITURL="http://cloud.github.com/downloads/KentBeck/"
JUNITZIP="junit-4.10.zip"
MATHURL="http://download.nextag.com/apache//commons/math/binaries/"
MATHTAR="commons-math-2.2.tar.gz"

#jfreechart-0.5.6, could not figure out what link to get from automatically

curl $JUNITURL/$JUNITJAR -o $JUNITJAR
curl $MATHURL/$MATHTAR -o $MATHTAR
gunzip $JUNITZIP
tar -xf $MATHTAR
