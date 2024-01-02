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
					if longValue, ok := valueMap["long"].(int64); ok {
						rawDatum[key] = longValue
					}
					if intValue, ok := valueMap["int"].(int32); ok {
						rawDatum[key] = intValue
					}
				}
			}

			// "webcastAddSales" 필드를 숫자로 변환
			webcastAddSalesStr, ok := rawDatum["webcastAddSales"].(string)
			if ok {
				webcastAddSales, err := strconv.ParseFloat(webcastAddSalesStr, 64)
				if err == nil {
					rawDatum["webcastAddSales"] = webcastAddSales
				}
			}

			// "webcastSalesMoney" 필드를 숫자로 변환
			webcastSalesMoneyStr, ok := rawDatum["webcastSalesMoney"].(string)
			if ok {
				webcastSalesMoney, err := strconv.ParseFloat(webcastSalesMoneyStr, 64)
				if err == nil {
					rawDatum["webcastSalesMoney"] = webcastSalesMoney
				}
			}

			// "price" 필드를 숫자로 변환
			priceStr, ok := rawDatum["price"].(string)
			if ok {
				price, err := strconv.ParseFloat(priceStr, 64)
				if err == nil {
					rawDatum["price"] = price
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
		dataMap := data.(map[string]interface{})
		productId, ok := dataMap["productId"].(string)
		if !ok {
			// productId가 없는 경우 오류 처리
			continue
		}
		metaData := map[string]interface{}{
			"update": map[string]interface{}{
				"_index": "products",
				"_id":    productId,
			},
		}
		jsonMeta, _ := json.Marshal(metaData)
		buffer.Write(jsonMeta)
		buffer.WriteString("\n")
		// 실제 데이터 작성
		doc := map[string]interface{}{
			"doc":           data,
			"doc_as_upsert": true, // 새 문서로 삽입하거나 기존 문서 업데이트
		}
		jsonData, _ := json.Marshal(doc)
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
