package surfstore

import (
	"errors"
	"log"
	"os"
	"reflect"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// DB-FILE-Location will be client.BaseDir + "/index.db"
	// TOMBSTONE := []string{TOMBSTONE_HASHVALUE}
	log.Println("Server is up and running.")
	DB_FILE_LOCATION := ConcatPath(client.BaseDir, DEFAULT_META_FILENAME)
	log.Println("DB_FILE_LOCATION: ", DB_FILE_LOCATION)
	// blocksize is specified by the client
	blockSize := client.BlockSize
	// check if .db file exists. if not, create
	if _, err := os.Stat(DB_FILE_LOCATION); errors.Is(err, os.ErrNotExist) {
		log.Println("Table did not exist, creating table")
		err = CreateTable(DB_FILE_LOCATION)
		if err != nil {
			log.Fatal(err)
		}
	}
	// load index file from the db
	prevFileMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Fatalln("Error in reading MetaFile: ", err.Error())
	}
	PrintMetaMap(prevFileMetaMap)
	// get all the entries in the base directory
	entries, err := os.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}

	fileHashlistMap := make(map[string][]string)
	localMetaMap := make(map[string]*FileMetaData)

	// iterate over all files
	for _, e := range entries {
		filename := e.Name()
		log.Println("Processing file: ", filename)
		fileloc := ConcatPath(client.BaseDir, filename)
		if filename == "index.db" || filename == ".DS_Store" {
			continue
		}
		b, err := os.ReadFile(fileloc)
		if err != nil {
			log.Printf("Error while reading file %s: %s", filename, err)
			continue
		}
		hashList := getHashlist(b, blockSize)
		fileHashlistMap[filename] = hashList
		log.Printf("%s is hashed\n", filename)
	}
	// here, we perform two checks
	// does a file currently present in our have a corresponding entry in the db. if no, push metadata of file to currMap with version1
	// if it is present in our database, check if both hashlists are equal
	// if equal, there is no change. currMap[filename] will be the prevMap[filename]
	// else, currMap[filename] will have the new hashList, and the version will be incremented by one from the prevMap[filename].Version
	for filename, hashList := range fileHashlistMap {
		if val, ok := prevFileMetaMap[filename]; ok {
			if reflect.DeepEqual(hashList, val.BlockHashList) {
				log.Println("File unchanged:", filename)
				localMetaMap[filename] = val
			} else {
				log.Println("File modified:", filename)
				log.Println("Hashes are: ")
				log.Println("Hashed blocks:", hashList)
				log.Println("Stored hashed blocks:", val)
				localMetaMap[filename] = &FileMetaData{Filename: filename, Version: val.Version + 1, BlockHashList: hashList}
			}
		} else {
			log.Println("New file:", filename)
			localMetaMap[filename] = &FileMetaData{Filename: filename, Version: 1, BlockHashList: hashList}
		}
	}

	// deletion of file. file is present in index.db, but absent in our scanned files
	for filename, _ := range prevFileMetaMap {
		if _, ok := fileHashlistMap[filename]; !ok {
			if len(prevFileMetaMap[filename].BlockHashList) == 1 && (prevFileMetaMap[filename].BlockHashList[0] == "0") {
				continue
			}
			localMetaMap[filename] = &FileMetaData{Filename: filename, Version: prevFileMetaMap[filename].Version + 1, BlockHashList: []string{"0"}}
		}
	}

	// PrintMetaMap(localMetaMap)

	// download remote index
	serverFileInfoMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&serverFileInfoMap)
	log.Println("localMetaMap")
	PrintMetaMap(localMetaMap)
	log.Println("Server file info map")
	PrintMetaMap(serverFileInfoMap)
	if err != nil {
		log.Println("HI IN ERROR BLOCK")
		log.Fatal(err.Error())
	}

	// compare between the two maps. decide whether to download file or upload file

	// first we will decide whether to download the file
	// this will be done by iterating over serverMap, and checking if there is an entry mentioned in serverMap which is not in our localMap
	// log.Println("Download")
	for remoteFilename, remoteMetaData := range serverFileInfoMap {
		if localMetaData, ok := localMetaMap[remoteFilename]; !ok { //file present on remote but not local
			// file deleted on remote, and not present on local as well
			if len(serverFileInfoMap[remoteFilename].BlockHashList) == 1 && (serverFileInfoMap[remoteFilename].BlockHashList[0] == "0") {
				// copy over the metadata with deleted information
				localMetaMap[remoteFilename] = &FileMetaData{}
				localMetaMap[remoteFilename] = remoteMetaData
				continue
			}
			download(client, remoteFilename, serverFileInfoMap)
			localMetaMap[remoteFilename] = &FileMetaData{}
			localMetaMap[remoteFilename] = remoteMetaData
		} else {
			if (localMetaData.Version < remoteMetaData.Version) || (localMetaData.Version == remoteMetaData.Version && !reflect.DeepEqual(localMetaData.BlockHashList, remoteMetaData.BlockHashList)) {
				download(client, remoteFilename, serverFileInfoMap)
				localMetaMap[remoteFilename] = &FileMetaData{}
				localMetaMap[remoteFilename] = remoteMetaData
			}
		}
	}
	// log.Println("Writing to database")
	// WriteMetaFile(serverFileInfoMap, client.BaseDir)

	// now, we will upload files

	// log.Println("Upload")
	for localFilename, localMetaData := range localMetaMap {
		if remoteMetaData, ok := serverFileInfoMap[localFilename]; !ok { //file present on remote but not local
			log.Printf("Uploading %s\n", localFilename)
			log.Println("MetaData in upload loop: ", localMetaMap[localFilename])
			upload(client, localFilename, localMetaMap)
		} else {
			if localMetaData.Version > remoteMetaData.Version {
				upload(client, localFilename, localMetaMap)
			}
		}
	}
	WriteMetaFile(localMetaMap, client.BaseDir)
}

func download(client RPCClient, filename string, remoteMap map[string]*FileMetaData) {

	// var blockStoreAddr *[]string = new([]string)
	// blockStoreMap := client.GetBlockStoreMap()
	// if err != nil {
	// 	log.Println(err.Error())
	// 	return
	// }

	remoteFileBlockList := remoteMap[filename].BlockHashList
	blockStoreMap := new(map[string][]string)
	client.GetBlockStoreMap(remoteFileBlockList, blockStoreMap)
	log.Println("processing file: ", filename)
	log.Println("remotefile block list: ", remoteFileBlockList)

	// map the hash of a block to the server where it is present
	hashToServerMap := make(map[string]string)
	for server, hashList := range *blockStoreMap {
		for _, hash := range hashList {
			hashToServerMap[hash] = server
		}
	}

	currFilename := ConcatPath(client.BaseDir, filename)
	remoteMetaData := remoteMap[filename]
	log.Println("Deleting file:", currFilename)
	if len(remoteMetaData.BlockHashList) == 1 && remoteMetaData.BlockHashList[0] == "0" {
		_, err := os.Stat(currFilename)
		if os.IsNotExist(err) {
			log.Println("File already deleted:", currFilename)
			return
		}
		// currFilename := ConcatPath(client.BaseDir, c)
		log.Println("deleting file: ", currFilename)
		err = os.Remove(currFilename)
		if err != nil {
			log.Fatal(err)
		}
		return
	}
	_, err := os.Stat(currFilename)
	if err == nil {
		err = os.Remove(currFilename)
		if err != nil {
			log.Fatal(err)
		}
	}
	file, err := os.Create(currFilename)
	if err != nil {
		log.Fatalf("Error while creating file: %s", err)
	}
	log.Println("File created at: ", currFilename)
	defer file.Close()

	var fileblocks string
	fileblocks = ""

	for _, hash := range remoteFileBlockList {
		var block Block
		server := hashToServerMap[hash]
		err = client.GetBlock(hash, server, &block)
		if err != nil {
			log.Fatal("Error while getting block: ", err.Error())
		}
		fileblocks += string(block.BlockData)
	}
	file.Write([]byte(fileblocks))
	// localMap = remoteMap
	// return localMap
}

func upload(client RPCClient, filename string, localMap map[string]*FileMetaData) error {
	// log.Println(filename)
	metaData := localMap[filename]
	log.Println("Metadata: ", metaData)

	path := ConcatPath(client.BaseDir, filename)
	var latestVersion *int32 = new(int32)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		err = client.UpdateFile(metaData, latestVersion)
		if err != nil {
			log.Println("Could not upload file: ", err)
		}
		metaData.Version = *latestVersion
		return err
	}

	file, err := os.Open(path)
	if err != nil {
		log.Println("Error opening file: ", err)
	}
	defer file.Close()

	b, _ := os.ReadFile(path)
	hashBlockMap := getBlocksAndHashes(b, client.BlockSize)
	hashes := getHashlist(b, client.BlockSize)
	blockStoreMap := new(map[string][]string)
	client.GetBlockStoreMap(hashes, blockStoreMap)

	for server, hashList := range *blockStoreMap {
		for _, hash := range hashList {
			blockData := hashBlockMap[hash]
			block := Block{BlockData: blockData, BlockSize: int32(len(blockData))}
			var succ bool
			if err := client.PutBlock(&block, server, &succ); err != nil {
				log.Println("Failed to put block: ", err)
			}
		}
	}
	log.Println("Metadata: ", metaData)
	err = client.UpdateFile(metaData, latestVersion)
	if err != nil {
		log.Println("Failed to update file: ", err)
		metaData.Version = -1
	}
	metaData.Version = *latestVersion

	return nil
}

// get map with key = hash, value = block
func getBlocksAndHashes(b []byte, blockSize int) map[string][]byte {
	blockMap := make(map[string][]byte)
	ptr := 0
	length := len(b)
	for ptr+blockSize < length {
		block := b[ptr : ptr+blockSize]
		ptr += blockSize
		hash := GetBlockHashString(block)
		blockMap[hash] = block
	}
	if ptr < length {
		block := b[ptr:]
		hash := GetBlockHashString(block)
		blockMap[hash] = block
	}
	return blockMap
}

// get hashes of a byte slice divided into blocks of size blockSize
func getHashlist(b []byte, blockSize int) []string {
	var hashList []string
	ptr := 0
	length := len(b)
	for ptr+blockSize < length {
		block := b[ptr : ptr+blockSize]
		ptr += blockSize
		hash := GetBlockHashString(block)
		hashList = append(hashList, hash)
	}
	if ptr < length {
		block := b[ptr:]
		hash := GetBlockHashString(block)
		hashList = append(hashList, hash)
	}
	return hashList
}

// func getBlocks(b []byte, blockSize int) [][]byte {
// 	var blockList [][]byte
// 	ptr := 0
// 	length := len(b)
// 	for ptr+blockSize < length {
// 		block := b[ptr : ptr+blockSize]
// 		ptr += blockSize
// 		blockList = append(blockList, block)
// 	}
// 	if ptr < length {
// 		block := b[ptr:]
// 		blockList = append(blockList, block)
// 	}
// 	return blockList
// }

// func initialInsertIntoDatabase()
