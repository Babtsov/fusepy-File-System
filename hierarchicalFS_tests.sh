cd ./fusemount
echo "----- recursive directory test -----------"
mkdir dir1
mkdir dir2
cd ./dir1
touch look
mkdir dir11
mkdir dir12
cd ./dir12
touch here
cd ../..
echo "Contents of the file system:"
pwd
ls -R
echo "----- END recursive directory test -------"
