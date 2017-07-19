package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ONSdigital/dp-publish-pipeline/health"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/ONSdigital/dp-publish-pipeline/vault"

	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"

	"database/sql"

	_ "github.com/lib/pq"
)

var (
	tick             = time.Millisecond * 330
	verboseTick      = false
	maxLaunchPerTick = 20
)

type dbMetaObj struct {
	db      *sql.DB
	prepped map[string]*sql.Stmt
}

type scheduleJob struct {
	scheduleId     int64
	collectionId   string
	collectionPath string
	scheduleTime   int64
	encryptionKey  string
	files          []utils.FileResource
	urisToDelete   []utils.FileResource
}

func publishCollection(job scheduleJob, fileProducerChannel, deleteProducerChannel, totalProducerChannel chan []byte) {
	if job.collectionId == "" {
		log.ErrorC("No collectionId", fmt.Errorf("job: %v", job), nil)
		panic("No collectionId")
	}

	var data []byte
	var err error

	// Send published files to the kafka topic
	for i := 0; i < len(job.files); i++ {
		if data, err = json.Marshal(utils.PublishFileMessage{
			ScheduleId:     job.scheduleId,
			FileId:         job.files[i].Id,
			CollectionId:   job.collectionId,
			CollectionPath: job.collectionPath,
			EncryptionKey:  job.encryptionKey,
			FileLocation:   job.files[i].Location,
			Uri:            job.files[i].Uri,
		}); err != nil {
			log.ErrorC("failed to marshal", err, nil)
			panic("failed to marshal")
		}
		fileProducerChannel <- data
	}
	log.Info(fmt.Sprintf("Job %d Collection %q sent %d files", job.scheduleId, job.collectionId, len(job.files)), nil)

	// Send published deletes to the kafka topic
	for i := 0; i < len(job.urisToDelete); i++ {
		if data, err = json.Marshal(utils.PublishDeleteMessage{
			ScheduleId:   job.scheduleId,
			DeleteId:     job.urisToDelete[i].Id,
			Uri:          job.urisToDelete[i].Uri,
			CollectionId: job.collectionId,
		}); err != nil {
			log.ErrorC("cannot marshal", err, nil)
			panic(err)
		}
		deleteProducerChannel <- data
	}
	log.Info(fmt.Sprintf("Job %d Collection %q sent %d deletes", job.scheduleId, job.collectionId, len(job.urisToDelete)), nil)
}

func scheduleCollection(jsonMessage []byte, dbMeta dbMetaObj) {
	var message utils.ScheduleMessage
	if err := json.Unmarshal(jsonMessage, &message); err != nil {
		log.ErrorC("Failed to parse json", err, log.Data{"msg": jsonMessage})
		panic(err)
	} else if len(message.CollectionId) == 0 || message.Action == "" {
		log.Error(fmt.Errorf("Empty collectionId/action"), log.Data{"msg": jsonMessage})
		panic("Empty collectionId/action")
	}

	scheduleTime, err := strconv.ParseInt(message.ScheduleTime, 10, 64)
	if err != nil {
		log.Error(fmt.Errorf("Collection %q Cannot numeric convert: %q", message.CollectionId, message.ScheduleTime), log.Data{"msg": message})
		panic("Cannot numeric convert")
	}
	scheduleTime *= 1000 * 1000 * 1000 // convert from epoch (seconds) to epoch-nanoseconds (UnixNano)

	if message.Action == "cancel" {
		cancelJob(dbMeta, message.CollectionId, scheduleTime)
	} else if message.Action == "schedule" {
		newJob := scheduleJob{
			collectionId:   message.CollectionId,
			collectionPath: message.CollectionPath,
			scheduleTime:   scheduleTime,
		}

		var files []utils.FileResource
		for i := 0; i < len(message.Files); i++ {
			files = append(files, utils.FileResource{Location: message.Files[i].Location, Uri: message.Files[i].Uri})
		}
		newJob.files = make([]utils.FileResource, len(files))
		copy(newJob.files, files)

		var deletes []utils.FileResource
		for i := 0; i < len(message.UrisToDelete); i++ {
			log.Trace(message.UrisToDelete[i], nil)
			deletes = append(deletes, utils.FileResource{Uri: message.UrisToDelete[i]})
		}
		newJob.urisToDelete = make([]utils.FileResource, len(deletes))
		copy(newJob.urisToDelete, deletes)

		newJob.scheduleId = storeJob(dbMeta, &newJob)
		log.Info(fmt.Sprintf("Job %d Collection %q scheduled: %d files, %d deletes", newJob.scheduleId, newJob.collectionId, len(newJob.files), len(newJob.urisToDelete)), nil)
	} else {
		log.Error(fmt.Errorf("Collection %q No/invalid action", message.CollectionId), log.Data{"msg": message})
		panic("No/invalid action")
	}
}

func checkSchedule(publishChannel chan scheduleJob, dbMeta dbMetaObj, restartGapNano int64, vaultClient *vault.VaultClient) {
	epochTime := time.Now().UnixNano()
	launchedThisTick := 0

	var rows *sql.Rows
	var err error
	if restartGapNano == 0 {
		rows, err = dbMeta.prepped["select-ready"].Query(epochTime)
	} else {
		rows, err = dbMeta.prepped["select-ready"].Query(epochTime, epochTime-restartGapNano)
	}
	if err != nil {
		log.Error(err, nil)
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			collectionId, collectionPath        sql.NullString
			scheduleId, startTime, scheduleTime sql.NullInt64
		)

		if err = rows.Scan(&scheduleId, &startTime, &scheduleTime, &collectionId, &collectionPath); err != nil {
			log.Error(err, nil)
			panic(err)
		}

		if maxLaunchPerTick > 0 && launchedThisTick >= maxLaunchPerTick {
			log.Info(fmt.Sprintf("Job %d Collection %q skip busy - this-tick: %d/%d", scheduleId.Int64, collectionId.String, launchedThisTick, maxLaunchPerTick), nil)
			continue
		}
		files := loadDataFromDataBase(dbMeta, scheduleId.Int64, false)
		deletes := loadDataFromDataBase(dbMeta, scheduleId.Int64, true)
		log.Trace(fmt.Sprintf("%d files, %d deletes", len(files), len(deletes)), nil)
		jobToGo := scheduleJob{
			scheduleId:     scheduleId.Int64,
			collectionId:   collectionId.String,
			collectionPath: collectionPath.String,
			scheduleTime:   scheduleTime.Int64,
			encryptionKey:  getEncryptionKeyFromVault(collectionId.String, vaultClient),
			files:          files,
			urisToDelete:   deletes,
		}
		publishChannel <- jobToGo
		launchedThisTick++
		log.Info(fmt.Sprintf("Job %d Collection %q launch#%d with %d files at time:%d", scheduleId.Int64, collectionId.String, launchedThisTick, len(files), epochTime), nil)
	}
	if verboseTick && launchedThisTick == 0 {
		log.Trace(fmt.Sprintf("No collections ready at %d", epochTime), nil)
	}
}

func loadDataFromDataBase(dbMeta dbMetaObj, scheduleId int64, loadDeletes bool) []utils.FileResource {
	sqlStmt := "load-incomplete-files"
	if loadDeletes {
		sqlStmt = "load-incomplete-deletes"
	}
	rows, err := dbMeta.prepped[sqlStmt].Query(scheduleId)
	if err != nil {
		log.Error(err, nil)
		panic(err)
	}
	defer rows.Close()

	var files []utils.FileResource
	for rows.Next() {
		var (
			fileId            sql.NullInt64
			fileLocation, uri sql.NullString
		)
		if err = rows.Scan(&fileId, &uri, &fileLocation); err != nil {
			log.Error(err, nil)
			panic(err)
		}

		files = append(files, utils.FileResource{Id: fileId.Int64, Location: fileLocation.String, Uri: uri.String})
	}
	if err = rows.Err(); err != nil {
		log.Error(err, nil)
		panic(err)
	}
	if loadDeletes {
		log.Info(fmt.Sprintf("Job %d Loaded %d deletes", scheduleId, len(files)), nil)
	} else {
		log.Info(fmt.Sprintf("Job %d Loaded %d files", scheduleId, len(files)), nil)
	}
	return files
}

func storeJob(dbMeta dbMetaObj, jobObj *scheduleJob) int64 {
	var (
		txn                           *sql.Tx
		fileStmt, jobStmt, deleteStmt *sql.Stmt
		err                           error
		scheduleId                    sql.NullInt64
		fileId                        int64
	)

	if txn, err = dbMeta.db.Begin(); err != nil {
		log.Error(err, nil)
		panic(err)
	}

	if jobStmt, err = txn.Prepare("INSERT INTO schedule (collection_id, collection_path, schedule_time, start_time, complete_time) VALUES ($1, $2, $3, NULL, NULL) RETURNING schedule_id"); err != nil {
		rollbackAndError(txn, err)
	}
	if fileStmt, err = txn.Prepare("INSERT INTO schedule_file (schedule_id, uri, file_location) VALUES ($1, $2, $3) RETURNING schedule_file_id"); err != nil {
		rollbackAndError(txn, err)
	}
	if deleteStmt, err = txn.Prepare("INSERT INTO schedule_delete (schedule_id, uri) VALUES ($1, $2) RETURNING schedule_delete_id"); err != nil {
		rollbackAndError(txn, err)
	}

	// insert job into schedule
	res := jobStmt.QueryRow((*jobObj).collectionId, (*jobObj).collectionPath, (*jobObj).scheduleTime)
	if err = res.Scan(&scheduleId); err != nil {
		rollbackAndError(txn, err)
	}
	// insert files into schedule_file
	for i := 0; i < len((*jobObj).files); i++ {
		if fileId, err = storeFile(fileStmt, scheduleId.Int64, (*jobObj).files[i].Uri, (*jobObj).files[i].Location); err != nil {
			rollbackAndError(txn, err)
		}
		(*jobObj).files[i].Id = fileId
	}

	// insert deleted files into schedule_delete
	for i := 0; i < len((*jobObj).urisToDelete); i++ {
		if _, err = storeFile(deleteStmt, scheduleId.Int64, (*jobObj).urisToDelete[i].Uri, ""); err != nil {
			rollbackAndError(txn, err)
		}
		(*jobObj).urisToDelete[i].Id = fileId
	}

	if err = txn.Commit(); err != nil {
		rollbackAndError(txn, err)
	}

	return scheduleId.Int64
}

func storeFile(stmt *sql.Stmt, scheduleId int64, uri, location string) (int64, error) {
	res := stmt.QueryRow(scheduleId, uri, location)
	var fileId sql.NullInt64
	if err := res.Scan(&fileId); err != nil {
		return 0, err
	}
	return fileId.Int64, nil
}

func rollbackAndError(txn *sql.Tx, err error) {
	if err2 := txn.Rollback(); err2 != nil {
		log.Error(fmt.Errorf("Error during rollback %s (while handling: %s)", err2, err), nil)
		panic(err)
	}
	log.Error(err, nil)
	panic(err)
}

func cancelJob(dbMeta dbMetaObj, collectionId string, scheduleTime int64) {
	var (
		txn        *sql.Tx
		jobStmt    *sql.Stmt
		err        error
		scheduleId sql.NullInt64
	)

	if txn, err = dbMeta.db.Begin(); err != nil {
		log.Error(err, nil)
		panic(err)
	}

	if jobStmt, err = txn.Prepare("DELETE FROM schedule WHERE collection_id=$1 AND schedule_time=$2 AND start_time IS NULL RETURNING schedule_id"); err != nil {
		rollbackAndError(txn, err)
	}

	// delete job(s) from schedule
	rows, err := jobStmt.Query(collectionId, scheduleTime)
	if err != nil {
		rollbackAndError(txn, err)
	}

	var scheduleIds []interface{}
	placeholder := ""
	for rows.Next() {
		if err = rows.Scan(&scheduleId); err != nil {
			rollbackAndError(txn, err)
		}
		scheduleIds = append(scheduleIds, scheduleId.Int64)
		if len(placeholder) > 0 {
			placeholder += ","
		}
		placeholder += "$" + strconv.Itoa(len(scheduleIds))
	}

	if len(scheduleIds) > 0 {
		// delete files from schedule_file
		if _, err := txn.Exec("DELETE FROM schedule_file WHERE schedule_id IN ("+placeholder+")", scheduleIds...); err != nil {
			rollbackAndError(txn, fmt.Errorf("Jobs %v Collection %q Cannot delete files: %s", scheduleIds, collectionId, err))
		}
		// delete files from schedule_delete
		if _, err := txn.Exec("DELETE FROM schedule_delete WHERE schedule_id IN ("+placeholder+")", scheduleIds...); err != nil {
			rollbackAndError(txn, fmt.Errorf("Jobs %v Collection %q Cannot delete file-deletes: %s", scheduleIds, collectionId, err))
		}

		log.Info(fmt.Sprintf("Jobs %v Collection %q at %d CANCELLED", scheduleIds, collectionId, scheduleTime), nil)

	} else {
		log.Info(fmt.Sprintf("Job ?? Collection %q at %d not found to cancel", collectionId, scheduleTime), nil)
	}

	if err = txn.Commit(); err != nil {
		rollbackAndError(txn, err)
	}
}

func getEncryptionKeyFromVault(collectionId string, vaultClient *vault.VaultClient) string {
	data, err := vaultClient.Read("secret/zebedee-cms/" + collectionId)
	if err != nil {
		log.ErrorC("Failed to find encryption key", err, nil)
		panic("Failed to find encryption key")
	}
	key, ok := data["encryption_key"].(string)
	if ok {
		return key
	}
	return ""
}

func (dbMeta dbMetaObj) prep(tag, sql string) {
	var err error
	dbMeta.prepped[tag], err = dbMeta.db.Prepare(sql)
	if err != nil {
		log.ErrorC("Could not prepare statement on database", err, log.Data{"tag": tag})
		panic("Could not prepare statement on database")
	}
}

func main() {
	log.Namespace = "publish-scheduler"
	maxMessageSize, err := utils.GetEnvironmentVariableInt("KAFKA_MESSAGE_SIZE", 157286400) // default to 150MB
	scheduleTopic := utils.GetEnvironmentVariable("SCHEDULE_TOPIC", "uk.gov.ons.dp.web.schedule")
	produceFileTopic := utils.GetEnvironmentVariable("PUBLISH_FILE_TOPIC", "uk.gov.ons.dp.web.publish-file")
	produceDeleteTopic := utils.GetEnvironmentVariable("PUBLISH_DELETE_TOPIC", "uk.gov.ons.dp.web.publish-delete")
	produceTotalTopic := utils.GetEnvironmentVariable("PUBLISH_COUNT_TOPIC", "uk.gov.ons.dp.web.publish-count")
	dbSource := utils.GetEnvironmentVariable("DB_ACCESS", "user=dp dbname=dp sslmode=disable")
	restartGap, err := utils.GetEnvironmentVariableInt("RESEND_AFTER_QUIET_SECONDS", 0)
	if err != nil {
		log.ErrorC("Failed to parse RESEND_AFTER_QUIET_SECONDS", err, nil)
		panic("Failed to parse RESEND_AFTER_QUIET_SECONDS")
	}
	restartGapNano := int64(restartGap * 1000 * 1000 * 1000)
	vaultToken := utils.GetEnvironmentVariable("VAULT_TOKEN", "")
	vaultAddr := utils.GetEnvironmentVariable("VAULT_ADDR", "http://127.0.0.1:8200")
	vaultRenewTime, err := utils.GetEnvironmentVariableInt("VAULT_RENEW_TIME", 5)
	healthCheckAddr := utils.GetEnvironmentVariable("HEALTHCHECK_ADDR", ":8080")
	healthCheckEndpoint := utils.GetEnvironmentVariable("HEALTHCHECK_ENDPOINT", "/healthcheck")
	brokers := utils.GetEnvironmentVariableAsArray("KAFKA_ADDR", "localhost:9092")

	vaultClient, err := vault.CreateVaultClient(vaultToken, vaultAddr)
	if err != nil {
		log.ErrorC("Failed to connect to vault", err, nil)
		panic("Failed to connect to vault")
	}

	db, err := sql.Open("postgres", dbSource)
	if err != nil {
		log.ErrorC("DB open error", err, nil)
		panic("DB open error")
	}
	if err = db.Ping(); err != nil {
		log.ErrorC("Could not establish a connection with the database", err, nil)
		panic("Could not establish a connection with the database")
	}
	dbMeta := dbMetaObj{db: db, prepped: make(map[string]*sql.Stmt)}
	dbMeta.prep("load-incomplete-files", "SELECT schedule_file_id, uri, file_location FROM schedule_file WHERE schedule_id=$1 AND complete_time IS NULL")
	dbMeta.prep("load-incomplete-deletes", "SELECT schedule_delete_id, uri FROM schedule_delete WHERE schedule_id=$1 AND complete_time IS NULL")
	dbMeta.prep("healthcheck", "SELECT 1 FROM schedule_delete")
	if restartGap == 0 {
		dbMeta.prep("select-ready", "UPDATE schedule s SET start_time=$1 WHERE complete_time IS NULL AND start_time IS NULL AND schedule_time <= $1 RETURNING schedule_id, start_time, schedule_time, collection_id, collection_path")
	} else {
		dbMeta.prep("select-ready", "UPDATE schedule s SET start_time=$1 WHERE complete_time IS NULL AND schedule_time <= $1 AND (start_time IS NULL OR (start_time <= $2 AND NOT EXISTS(SELECT 1 FROM schedule_file sf WHERE s.schedule_id=sf.schedule_id AND sf.complete_time > $2))) RETURNING schedule_id, start_time, schedule_time, collection_id, collection_path")
	}

	log.Info(fmt.Sprintf("Starting publish scheduler topics: %q -> %q/%q/%q", scheduleTopic, produceFileTopic, produceDeleteTopic, produceTotalTopic), nil)

	kafka.SetMaxMessageSize(int32(maxMessageSize))
	totalProducer := kafka.NewProducer(brokers, produceTotalTopic, maxMessageSize)
	scheduleConsumer, err := kafka.NewConsumerGroup(brokers, scheduleTopic, "publish-scheduler", kafka.OffsetNewest)
	if err != nil {
		log.ErrorC("Could not obtain consumer", err, nil)
		panic("Could not obtain consumer")
	}
	fileProducer := kafka.NewProducer(brokers, produceFileTopic, maxMessageSize)
	deleteProducer := kafka.NewProducer(brokers, produceDeleteTopic, maxMessageSize)

	publishChannel := make(chan scheduleJob)
	healthChannel := make(chan bool)
	exitChannel := make(chan bool)

	go func() {
		tock := time.Tick(tick)
		for _ = range tock {
			checkSchedule(publishChannel, dbMeta, restartGapNano, vaultClient)
		}
	}()

	go func() {
		tock := time.Tick(time.Duration(vaultRenewTime) * time.Minute)
		for _ = range tock {
			err := vaultClient.Renew()
			if err != nil {
				log.ErrorC("Failed to renew vault token", err, nil)
				panic("Failed to renew vault token")
			}
			log.Trace("Renewed vault token", nil)
		}
	}()

	go func() {
		http.HandleFunc(healthCheckEndpoint, health.NewHealthChecker(healthChannel, dbMeta.prepped["healthcheck"]))
		log.Info(fmt.Sprintf("Listening for %s on %s", healthCheckEndpoint, healthCheckAddr), nil)
		log.ErrorC("healthcheck listener exited", http.ListenAndServe(healthCheckAddr, nil), nil)
		panic("healthcheck listener exited")
	}()

	go func() {
		for {
			select {
			case scheduleMessage := <-scheduleConsumer.Incoming():
				scheduleCollection(scheduleMessage.GetData(), dbMeta)
				scheduleMessage.Commit()
			case publishMessage := <-publishChannel:
				go publishCollection(publishMessage, fileProducer.Output(), deleteProducer.Output(), totalProducer.Output())
			case <-healthChannel:
			case errorMessage := <-scheduleConsumer.Errors():
				log.Error(fmt.Errorf("Aborting"), log.Data{"messageReceived": errorMessage})
				scheduleConsumer.Closer() <- true
				exitChannel <- true
				return
			}
		}
	}()
	<-exitChannel
	log.Info("Service publish scheduler stopped", nil)
}
