LIBFOLDER="lib"
JUNITURL="http://cloud.github.com/downloads/KentBeck/"
JUNITJAR="junit-4.10.jar"
MATHURL="http://download.nextag.com/apache//commons/math/binaries/"
MATHTAR="commons-math-2.2.tar.gz"


if [[ ! -d $LIBFOLDER ]]; then
mkdir $LIBFOLDER
fi

cd $LIBFOLDER
curl $JUNITURL/$JUNITJAR -o $JUNITJAR
curl $MATHURL/$MATHTAR -o $MATHTAR
tar -xf $MATHTAR
