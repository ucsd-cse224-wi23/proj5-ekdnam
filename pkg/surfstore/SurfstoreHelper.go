package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
// return hash of a byte slice
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

// return hash of byte slice encoded to string
func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes values (?, ?, ?, ?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	log.Println("Table created")
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	statement2, err := db.Prepare(insertTuple)
	// log.Println(statement2)
	if err != nil {
		log.Fatal("Error while preparing to insert data into table")
	}
	for filename, filemeta := range fileMetas {
		for index, hash := range filemeta.BlockHashList {
			log.Printf("Executing insert statement with: %s %d %d %s", filename, filemeta.Version, index, hash)
			statement2.Exec(filename, filemeta.Version, index, hash)
		}
	}
	return nil
}

// create table <file>.db
func CreateTable(outputMetaPath string) error {
	// remove index.db file if it exists
	// outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	return nil
}

/*
Reading Local Metadata File Related
*/
// const getDistinctFileName string = "select distinct(filename) from indexes"

// const getTuplesByFileName string = "select * from indexes where filename = ?"

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (map[string]*FileMetaData, error) {
	// log.Println("In Load Meta File")
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap := make(map[string]*FileMetaData)

	metaFileStats, e := os.Stat(metaFilePath)

	if e != nil || metaFileStats.IsDir() {
		log.Println(e.Error())
		return fileMetaMap, nil
	}

	log.Println("Opening db")

	database, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		// log.Fatal("Error When Opening Meta")
		return fileMetaMap, err
	}

	readQuery := fmt.Sprintf("select * from  %s order by filename asc, hashIndex asc;", "indexes")
	rows, err := database.Query(readQuery)
	// log.Println("Database is queried")
	if err != nil {
		return fileMetaMap, err
	}
	// err, filenames, versions, hashIndices, hashValues := readAllFromTable(database, readQuery)
	// err = processSlices(fileMetaMap, filenames, versions, hashIndices, hashValues)
	// log.Println("Reading from Database")
	var filename string
	var version int
	var hashIndex int
	var hashValue string
	for rows.Next() {
		rows.Scan(&filename, &version, &hashIndex, &hashValue)
		log.Println("Retrieved from DB\t", filename, version, hashIndex, hashValue)
		if _, ok := fileMetaMap[filename]; !ok {
			fileMetaMap[filename] = &FileMetaData{}
		}
		fileMetaMap[filename].Filename = filename
		fileMetaMap[filename].Version = int32(version)
		fileMetaMap[filename].BlockHashList = append(fileMetaMap[filename].BlockHashList, hashValue)
		// log.Println("Values inserted in map.")
	}
	return fileMetaMap, err
}

// func insertTupleToDB(client RPCClient, filename string, version int, hashList []string) error {
// 	metaFilePath, _ := filepath.Abs(ConcatPath(client.BaseDir, DEFAULT_META_FILENAME))
// 	metaFileStats, e := os.Stat(metaFilePath)
// 	if e != nil || metaFileStats.IsDir() {
// 		return e
// 	}
// 	database, err := sql.Open("sqlite3", metaFilePath)
// 	if err != nil {
// 		// log.Fatal("Error When Opening Meta")
// 		return err
// 	}
// 	query := "INSERT INTO indexes (filename, version, hashIndex, hashValue) VALUES (?, ?, ?, ?)"
// 	statement, _ := database.Prepare(query)
// 	for i, hash := range hashList {
// 		statement.Exec(filename, version, i, hash)
// 	}
// 	return nil
// }

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")
	TOMBSTONE := []string{TOMBSTONE_HASHVALUE}

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			if reflect.DeepEqual(blockHash, TOMBSTONE) {
				fmt.Println("\t", blockHash)
			}
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
