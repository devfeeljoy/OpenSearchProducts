package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/linkedin/goavro/v2"
	"net/http"
	"os"
	"strconv"
)

func HandleRequest(ctx context.Context, s3Event events.S3Event) {
	openSearchURL := os.Getenv("OPENSEARCH_URL")

	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("ap-northeast-2")}, // AWS 리전 설정
	)

	s3Client := s3.New(sess)

	for _, record := range s3Event.Records {

		bucket := record.S3.Bucket.Name
		key := record.S3.Object.Key
		//bucket := "burige.314257712735.ap-northeast-2"
		//key := "NewRank/2023-12-01/buridge_dy_account_daily_data_20231201.avro"

		// S3에서 Avro 파일 가져오기
		result, err := s3Client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			fmt.Printf("Error getting Avro file from S3: %s\n", err)
			return
		}
		bodyReader := bufio.NewReader(result.Body)

		// Avro 파일 읽기 및 처리
		ocfr, err := goavro.NewOCFReader(bodyReader)
		if err != nil {
			fmt.Printf("Error creating OCF reader: %s\n", err)
			return
		}
		// HandleRequest 함수 내에서
		var batchData []interface{}
		// Avro 레코드 처리
		for ocfr.Scan() {
			avroRecord, err := ocfr.Read()
			if err != nil {
				fmt.Println("Error reading datum:", err)
				continue
			}

			// 타입 단언을 사용하여 rawDatum을 map[string]interface{} 타입으로 변환
			rawDatum, ok := avroRecord.(map[string]interface{})
			if !ok {
				fmt.Println("Error asserting datum to map[string]interface{}")
				continue
			}

			// 필요한 데이터 변환 수행
			for key, value := range rawDatum {

				if valueMap, ok := value.(map[string]interface{}); ok {
					if stringValue, ok := valueMap["string"].(string); ok {
						rawDatum[key] = stringValue
					}
				}

				// "age" 필드를 숫자로 변환
				ageStr, ok := rawDatum["age"].(string)
				if ok {
					age, err := strconv.Atoi(ageStr)
					if err == nil {
						rawDatum["age"] = age
					}
				}

				// "dyFansNum" 필드를 숫자로 변환
				dyFansNumStr, ok := rawDatum["dyFansNum"].(string)
				if ok {
					dyFansNum, err := strconv.Atoi(dyFansNumStr)
					if err == nil {
						rawDatum["dyFansNum"] = dyFansNum
					}
				}

				// "fansNum" 필드를 숫자로 변환
				fansNumStr, ok := rawDatum["fansNum"].(string)
				if ok {
					fansNum, err := strconv.Atoi(fansNumStr)
					if err == nil {
						rawDatum["fansNum"] = fansNum
					}
				}

				// "likeNum" 필드를 숫자로 변환
				likeNumStr, ok := rawDatum["likeNum"].(string)
				if ok {
					likeNum, err := strconv.Atoi(likeNumStr)
					if err == nil {
						rawDatum["likeNum"] = likeNum
					}
				}

				// "opusNum" 필드를 숫자로 변환
				opusNumStr, ok := rawDatum["opusNum"].(string)
				if ok {
					opusNum, err := strconv.Atoi(opusNumStr)
					if err == nil {
						rawDatum["opusNum"] = opusNum
					}
				}
			}
			batchData = append(batchData, rawDatum)

			// 배치 크기에 도달하거나 마지막 레코드인 경우 색인화
			if len(batchData) >= 1000 {
				err = indexBatchToOpenSearch(batchData, openSearchURL)
				if err != nil {
					fmt.Printf("Error indexing batch to OpenSearch: %s\n", err)
				}
				batchData = nil // 배치 초기화
			}
		}
		if len(batchData) > 0 {
			err = indexBatchToOpenSearch(batchData, openSearchURL)
			if err != nil {
				fmt.Printf("Error indexing batch to OpenSearch: %s\n", err)
			}
		}

	}
}

func indexBatchToOpenSearch(batchData []interface{}, openSearchURL string) error {

	// 환경 변수에서 OpenSearch의 사용자 이름과 비밀번호를 읽습니다.
	username := os.Getenv("OPENSEARCH_USERNAME")
	password := os.Getenv("OPENSEARCH_PASSWORD")

	//signer *v4.Signer
	var buffer bytes.Buffer
	for _, data := range batchData {
		metaData := map[string]map[string]string{
			"index": {"_index": "influencer"},
		}
		jsonMeta, _ := json.Marshal(metaData)
		buffer.Write(jsonMeta)
		buffer.WriteString("\n")

		jsonData, _ := json.Marshal(data)
		buffer.Write(jsonData)
		buffer.WriteString("\n")
	}

	req, _ := http.NewRequest("POST", openSearchURL+"/_bulk", &buffer)

	// ID와 패스워드를 결합하고 Base64로 인코딩합니다.
	auth := username + ":" + password
	authEncoded := base64.StdEncoding.EncodeToString([]byte(auth))

	// Authorization 헤더를 설정합니다.
	req.Header.Set("Authorization", "Basic "+authEncoded)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending bulk request to OpenSearch: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error response from OpenSearch: %v", resp.Status)
	}

	return nil
}

func main() {
	lambda.Start(HandleRequest)
}
