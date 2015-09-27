cd ./fusemount
echo "------------------------- File system tests -----------------------------"
echo "Make directories and subdirectories"
# create 2 directories
mkdir dir1
mkdir dir2
# add content to dir2
mkdir dir2/dir21
touch dir2/file1.txt
echo "I'm in dir2!" > dir2/coco.txt
# add content to dir1 by 
cd ./dir1
touch look.txt
echo "I'm in dir1" > here.txt
mkdir dir11
mkdir dir12
cd ./dir12
touch here
# return back to root and print file system
cd ../..
echo "Contents of the file system:"
pwd
ls -R

echo
echo "Rename dir2 to DIR2 and move ./dir1/look.txt from dir1 to ./DIR2/dir21/"
mv dir2 DIR2
mv ./dir1/look.txt ./DIR2/dir21/
echo "Contents of the file system:"
pwd
ls -R

echo
echo "Recursively remove DIR2 files and contents"
rm -R DIR2
echo "Contents of the file system:"
pwd
ls -R
echo "------------------------- END File system tests --------------------------"

