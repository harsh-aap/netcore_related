package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var httpClient = &http.Client{
	Timeout: 15 * time.Second,
}

var searchMu sync.Mutex

/*
	 =========================
		Process Contact

=========================
*/
func ProcessContact(ctx context.Context, contact Contact) {

	log.Println("🔍 Searching contact:", contact.Phone)
	found, contactID, err := SearchContactAPI(ctx, contact.Phone)
	if err != nil {
		log.Println("❌ Search error for", contact.Phone, ":", err)
		return
	}

	if found {
		log.Println("✅ Contact found:", contactID, "→ updating")
		id, _ := strconv.Atoi(contactID)
		UpdateQueue <- PendingUpdate{ContactID: id, Contact: contact}

		//previous logic was to call the api here
		// err = UpdateContactAPI(ctx, contactID, contact)
		// if err != nil {
		// 	log.Println("❌ Update error for", contact.Phone, ":", err)
		// } else {
		// 	log.Println("✅ Update success for", contact.Phone)
		// }

	} else {
		log.Println("⚠ Contact not found → creating:", contact.Phone)
		CreateQueue <- contact

		//previous logic was to call the api here
		// err = CreateContactAPI(ctx, contact)
		// if err != nil {
		// 	log.Println("❌ Create error for", contact.Phone, ":", err)
		// } else {
		// 	log.Println("✅ Create success for", contact.Phone)
		// }

	}
}

/* =========================
	Search Contact API
========================= */

type SearchRequest struct {
	Output struct {
		GetCount bool     `json:"get_count"`
		Fields   []string `json:"fields"`
	} `json:"output"`
	FilteringCriteria []struct {
		ConditionDetails []struct {
			Field         string   `json:"field"`
			FieldCategory string   `json:"field_category"`
			Operation     string   `json:"operation"`
			Value         []string `json:"value"`
		} `json:"condition_details"`
	} `json:"filtering_criteria"`
}

type SearchResponse struct {
	Data []struct {
		ContactID int    `json:"contact_id"`
		Email     string `json:"email"`
		Mobile    string `json:"mobile"`
	} `json:"data"`
}

func SearchContactAPI(ctx context.Context, phone string) (bool, string, error) {

	searchMu.Lock()
	defer searchMu.Unlock()

	time.Sleep(1 * time.Second)

	log.Println("🔍 Starting search for contact with phone:", phone)

	// Build request body
	reqBody := SearchRequest{}
	reqBody.Output.GetCount = false
	reqBody.Output.Fields = []string{"mobile", "email", "contact_id"}

	log.Printf("📦 Preparing search filter for phone: %s\n", phone)
	condition := struct {
		Field         string   `json:"field"`
		FieldCategory string   `json:"field_category"`
		Operation     string   `json:"operation"`
		Value         []string `json:"value"`
	}{
		Field:         "MOBILE",
		FieldCategory: "config",
		Operation:     "equals",
		Value:         []string{phone},
	}

	filter := struct {
		ConditionDetails []struct {
			Field         string   `json:"field"`
			FieldCategory string   `json:"field_category"`
			Operation     string   `json:"operation"`
			Value         []string `json:"value"`
		} `json:"condition_details"`
	}{
		ConditionDetails: []struct {
			Field         string   `json:"field"`
			FieldCategory string   `json:"field_category"`
			Operation     string   `json:"operation"`
			Value         []string `json:"value"`
		}{condition},
	}

	reqBody.FilteringCriteria = []struct {
		ConditionDetails []struct {
			Field         string   `json:"field"`
			FieldCategory string   `json:"field_category"`
			Operation     string   `json:"operation"`
			Value         []string `json:"value"`
		} `json:"condition_details"`
	}{filter}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		log.Println("❌ Error marshaling request body:", err)
		return false, "", err
	}

	log.Println("📤 Sending HTTP POST request to search API")
	req, err := http.NewRequestWithContext(ctx, "POST", BaseURL+"/contact/search", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Println("❌ Error creating HTTP request:", err)
		return false, "", err
	}

	req.Header.Set("api-key", APIKey)
	req.Header.Set("accept", "application/json")
	req.Header.Set("content-type", "application/json")

	start := time.Now()
	resp, err := httpClient.Do(req)
	duration := time.Since(start)

	if err != nil {
		log.Println("❌ HTTP request failed:", err)
		return false, "", err
	}
	defer resp.Body.Close()

	log.Printf("📥 Response received (status: %d) in %v\n", resp.StatusCode, duration)

	if resp.StatusCode == 401 {
		log.Println("❌ Unauthorized: check API key")
		return false, "", fmt.Errorf("unauthorized (401)")
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("❌ Search API returned non-OK status: %d\n", resp.StatusCode)
		return false, "", fmt.Errorf("search failed with status: %d", resp.StatusCode)
	}

	var result SearchResponse
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		log.Println("❌ Error decoding search response:", err)
		return false, "", err
	}

	log.Printf("🔎 Search response: %+v\n", result)

	if len(result.Data) > 0 {
		contactID := fmt.Sprintf("%d", result.Data[0].ContactID) // convert int → string
		log.Println("✅ Contact found with phone:", phone, "ContactID:", contactID)
		return true, contactID, nil
	}

	log.Println("⚠ Contact NOT found for phone:", phone)
	return false, "", nil
}

/*
	 =========================
		Update Contact API

=========================
*/
func BulkUpdateAPI(ctx context.Context, items []PendingUpdate) error {
	contactList := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		entry := map[string]interface{}{
			"contact_id": item.ContactID,
			"mobile":     item.Contact.Phone,
		}
		attrs := buildAttributes(item.Contact)
		if len(attrs) > 0 {
			entry["attributes"] = attrs
		}
		contactList = append(contactList, entry)
	}

	payload := map[string]interface{}{
		"data": map[string]interface{}{
			"contact_type": "identified",
			"contacts":     contactList,
		},
	}

	jsonData, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		log.Println("❌ Error marshaling bulk update payload:", err)
		return err
	}

	log.Println("📤 Sending Bulk Update API request")
	log.Printf("📄 Payload:\n%s\n", string(jsonData))

	req, err := http.NewRequestWithContext(ctx, "POST", BaseURL+"/contact/update", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Api-Key", APIKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Println("❌ HTTP request failed:", err)
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	log.Printf("📥 Bulk Update response (status: %d) in %v\n%s\n", resp.StatusCode, time.Since(start), string(respBody))

	if resp.StatusCode >= 300 {
		return fmt.Errorf("bulk update failed with status: %d", resp.StatusCode)
	}
	return nil
}

/*
	 =========================
		Create Contact API

=========================
*/
func BulkCreateAPI(ctx context.Context, contacts []Contact) error {
	contactList := make([]map[string]interface{}, 0, len(contacts))
	for _, c := range contacts {
		entry := map[string]interface{}{
			"mobile":   c.Phone,
			"identity": c.Phone,
		}
		attrs := buildAttributes(c)
		if len(attrs) > 0 {
			entry["attributes"] = attrs
		}
		contactList = append(contactList, entry)
	}

	payload := map[string]interface{}{
		"data": map[string]interface{}{
			"contact_type": "identified",
			"contacts":     contactList,
		},
	}

	jsonData, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		log.Println("❌ Error marshaling bulk create payload:", err)
		return err
	}

	log.Println("📤 Sending Bulk Create API request")
	log.Printf("📄 Payload:\n%s\n", string(jsonData))

	req, err := http.NewRequestWithContext(ctx, "POST", BaseURL+"/contact/create", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Api-Key", APIKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Println("❌ HTTP request failed:", err)
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	log.Printf("📥 Bulk Create response (status: %d) in %v\n%s\n", resp.StatusCode, time.Since(start), string(respBody))

	if resp.StatusCode >= 300 {
		return fmt.Errorf("bulk create failed with status: %d", resp.StatusCode)
	}
	return nil
}
