rm -r fusemount
rm -r test_result 
mkdir fusemount
mkdir test_result 
#cd fusemount
 

########################################################
#   DIRECTORY CREATE TEST #
######################################################## 
echo "-----------------------DATABASE FILE SYSTEM TEST  -----------------------------"
mkdir_test(){
echo "***TESTING  MKDIR***"
root=./fusemount/
# create 5  directories
   
  for ((i = 0; i < 5; i++))
   do
     file_name=dir$i
     mkdir $root$file_name 
   done

echo "***TESTING  FILE SYSTEM DEPTH***"
path_name="./fusemount/dir0/"   
   for ((i = 0; i <10; i++))
   do
     file_name="dir0_$i/"
     file_name=$path_name$file_name
     mkdir $file_name 
     path_name=$file_name 
   done
echo "***File Structure Check***"
ls -R ./fusemount >> result_mkdir.txt 
}

########################################################
#   FILE CREATE, READ, WRITE TEST  
#   for various subdir/dir depth
######################################################## 
fileCreate_test(){
echo "***TESTING  FILE CREATION FOR VARIOUS DEPTH***"
echo "***TESTING  FILE CREATION DEPTH = 6***">>result_fileCreate.txt
path_1=./fusemount/dir0/dir0_0/dir0_1/dir0_2/dir0_3/dir0_4/dir0_5/textfile0_5.txt
echo "Hi File Creation Success Depth = 6" > $path_1
cat $path_1 1>result_fileCreate.txt


echo "***TESTING  FILE CREATION DEPTH = 10***">>result_fileCreate.txt
path_1=./fusemount/dir0/dir0_0/dir0_1/dir0_2/dir0_3/dir0_4/dir0_5/dir0_6/dir0_7/dir0_8/dir0_9/textfile0_9.txt
echo "Hi File Creation Success Depth = 6" > $path_1
cat $path_1 1>result_fileCreate.txt

echo "***TESTING  FILE CREATION DEPTH = 0***">>result_fileCreate.txt
path_1=./fusemount/textfile0_root.txt
echo "Hi File Creation Success Depth = 0" > $path_1
cat $path_1 1>result_fileCreate.txt
}


########################################################
#   CREATING SYMBOLIC LINK  
#   for various subdir/dir depth
########################################################
SymlinkCreate_test(){
echo "***TESTING SYMBOLIC LINK******"
echo "***TESTING SYMBOLIC LINK  FILE******">result_FileSymLink.txt
path_1=./fusemount/dir0/dir0_0/dir0_1/dir0_2/dir0_3/dir0_4/dir0_5/dir0_6/dir0_7/dir0_8/dir0_9/textfile0_9.txt
path_new=./fusemount/textfile0_9_symRoot.txt
ln -s $path_1 $path_new
echo "Depth 10->Root==File">>result_FileSymLink.txt 
ls -l ./fusemount >result_FileSymLink.txt

path_1=./fusemount/dir0/dir0_0/dir0_1/dir0_2/dir0_3/dir0_4/dir0_5/dir0_6/dir0_7/dir0_8/dir0_9/textfile0_9.txt
path_new=./fusemount/dir0/dir0_0/dir0_1/textfile0_1_symRoot.txt
ln -s $path_1 $path_new 
echo "Depth 10->Depth 2==File">>result_FileSymLink.txt 
ls -l ./fusemount/dir0/dir0_0/dir0_1 >>result_FileSymLink.txt


echo "***TESTING SYMBOLIC LINK DIRECTORY******">>result_FileSymLink.txt
path_1=./fusemount/dir0/dir0_0/dir0_1/dir0_2/dir0_3/dir0_4/dir0_5/dir0_6/dir0_7/
path_new=./fusemount/dir1
ln -s $path_1 $path_new 
echo "Depth 8->Depth 1==Dir">>result_FileSymLink.txt 
ls -l $path_new >>result_FileSymLink.txt
}


########################################################
#   MOVE/COPY TEST  
#   for various subdir/dir depth
########################################################
move_copy_test(){
echo "***TESTING COPY******"
path_from=./fusemount/dir0/dir0_0/dir0_1/dir0_2/dir0_3/dir0_4/dir0_5/dir0_6/dir0_7/dir0_8/
path_to=./fusemount/dir2/
cp -r  $path_from $path_to
echo "***Copying directories with sub directories and files***"
echo "***Copying directories with sub directories and files***">move_copy.txt
ls -l $path_to >> move_copy.txt


echo "***TESTING MOVE******"
path_to=./fusemount/dir0/dir0_0/dir0_1/dir0_2/dir0_3/dir0_4/dir0_5/dir0_6/dir0_7/dir0_8/
path_from=./fusemount/textfile0_9_symRoot.txt
mv   $path_from $path_to
echo "***Moving symbolic link files***"
echo "***Moving symbolic link files***">>move_copy.txt
ls -l $path_to >> move_copy.txt

}


########################################################
#   REMOVE FILE/DIRECTORY TEST  
#   for various subdir/dir depth
########################################################
remove_test(){
echo "****TESTING REMOVE****"
echo "***Before Remove Directories***">remove_test.txt 
ls ./fusemount > remove_test.txt
echo "***After Remove Directories***">>remove_test.txt 
rm -r ./fusemount/dir4/
ls ./fusemount >> remove_test.txt
}



 mkdir_test
 fileCreate_test
 SymlinkCreate_test
 move_copy_test
 remove_test
 mv *.txt test_result

