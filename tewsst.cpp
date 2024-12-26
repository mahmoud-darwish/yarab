#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <sstream>
#include <cstring>
#include <fstream>
#include <filesystem>
#include <set>
#include <algorithm>
#include <unordered_set> 
#include <optional>
namespace fs = std::filesystem;
using namespace  std;

// Constants
constexpr size_t PAGE_SIZE = 4096; // 4 KB

// Slot structure represents a tuple's metadata location
struct Slot {
    uint16_t offset;  // Offset of the tuple in the page
    uint16_t length;  // Length of the tuple
};

// Tuple class for dynamic schema logic
class Tuple {
private:
    std::vector<std::pair<std::string, std::pair<int, std::string>>> attributes;

public:

    void addAttribute(const std::string& key, int type, const std::string& value) {
        // for (auto& attr : attributes) {
        //     if (attr.first == key) {
        //         attr.second = {type, value};
        //         std::cout << "[DEBUG addAttribute] Updated attribute: " << key << " with value: " << value << std::endl;
 
        //         return;
        //     }
        // }
        attributes.push_back({key, {type, value}});
        std::cout << "[DEBUG addAttribute] Added new attribute: " << key << " with value: " << value << std::endl;
 
    }

std::string serialize() const {
    std::ostringstream oss;
    for (const auto& attr : attributes) {
        // Serialize attr.first, attr.second.first, and attr.second.second
        oss << attr.first << "(" << attr.second.first << "|" << attr.second.second << ")";
    }
    std::string result = oss.str();
    std::cout << "[DEBUG Tuple serialize] Serialized tuple: " << result << std::endl;
    return result;
}

bool deserialize(const std::string& data) {
    attributes.clear();  // Clear the attributes vector to start fresh
    std::istringstream iss(data);  // Create a stringstream from the input data
    std::string token;  // To store each token extracted from the data

    // Process the data string token by token, delimited by ';'
    while (std::getline(iss, token, ')')) {
        if (token.empty()) continue;  // Skip empty tokens (just in case)

        // Find the position of the colon ':' in the token
        auto colonPos = token.find('(');
        if (colonPos == std::string::npos) {
            std::cerr << "[ERROR Tuple deserialize] Malformed token: " << token << std::endl;
            continue;
        }

        // Extract the key
        std::string key = token.substr(0, colonPos);

        // Extract the part after the colon and split by comma to get the values
        std::string values = token.substr(colonPos + 1);
        auto commaPos = values.find('|');
        if (commaPos == std::string::npos) {
            std::cerr << "[ERROR Tuple deserialize] Malformed value part: " << values << std::endl;
            continue;
        }

        // Extract first and second parts of the value
        std::string valueFirst = values.substr(0, commaPos);
        std::string valueSecond = values.substr(commaPos + 1);

        // If all components are valid, add them as an attribute
        if (!key.empty() && !valueFirst.empty() && !valueSecond.empty()) {
            try {
                int firstValue = std::stoi(valueFirst);  // Convert the first part to an integer
                addAttribute(key, firstValue, valueSecond);
            } catch (const std::exception& e) {
                std::cerr << "[ERROR Tuple deserialize] Failed to convert valueFirst to int: " << valueFirst << ". Exception: " << e.what() << std::endl;
            }
        } else {
            std::cerr << "[ERROR Tuple deserialize] Invalid key, valueFirst, or valueSecond: " 
                      << key << " - " << valueFirst << " - " << valueSecond << std::endl;
        }
    }

    bool success = !attributes.empty();
    std::cout << "[DEBUG Tuple deserialize] Deserialization " << (success ? "succeeded" : "failed") 
              << ". Total attributes: " << attributes.size() << std::endl;
    return success;
}


std::map<std::string, std::pair<int, std::string>> getAttributes() const {
    // Create an empty map to store the attributes
    std::map<std::string, std::pair<int, std::string>> attributesMap;

    // Iterate through each attribute in the 'attributes' vector
    for (const auto& attr : attributes) {
        // For each attribute, insert it into the map. The key is 'attr.first' (the attribute name),
        // and the value is 'attr.second' (the pair containing the type and value)
        attributesMap[attr.first] = attr.second;
    }

    // Return the populated map
    return attributesMap;
}
    std::string getAttributeValue(const std::string& key) const {
    for (const auto& attr : attributes) {
        std::cout << "[DEBUG getAttributeValue] Found attribute: " << key << " with value: " << attr.second.second << std::endl;
        if (attr.first == key) {
            return attr.second.second; // Return the value of the key
        }
    }
    std::cerr << "[WARNING getAttributeValue] Attribute not found: " << key << std::endl;

    return ""; // Key not found
    }
};

class FileMetadata {
private:
    static const int SCHEMA_SIZE = 512;        // Fixed size for schema
    static const int RESERVED_SIZE = 508;     // Reserved for future use
    //static const int MAP_ENTRIES = 896;       // 7 KB / 8 bytes per (tuple_id, page_id)
    static const int METADATA_SIZE = 8192;    // Total metadata size (8 KB)

    // Schema and number of pages in the file
    std::map<std::string, std::string> schema; // Maps attribute name to its type (e.g., "id" -> "int")
    uint16_t pageCount=0;
    char reserved[RESERVED_SIZE]={0};             // Reserved for future features
    std::map<int, int> tupleToPageMap;


public:
    FileMetadata() {
        // Initialize reserved space with zeros
        std::memset(reserved, 0, RESERVED_SIZE);
    }

    // Set the schema for the table
    void setSchema(const std::map<std::string, std::string>& tableSchema) {
        schema = tableSchema;
    }

    // Set the number of pages in the file
    void setPageCount(uint16_t count) {
        pageCount = count;
    }

     // Member variable to keep track of the next page ID
    uint32_t nextPageID = 1;

    // Method to get the next page ID
    uint32_t getNextPageID() {
        return nextPageID;
    }

    void incrementPageID() {
    if (nextPageID >= pageCount) {
        std::cerr << "Warning incrementPageID: Attempting to access invalid page ID: " << nextPageID << "\n";
        pageCount++;  // Ensure page count is incremented when creating new pages
    }
    nextPageID++;
}


    // Add a tuple-to-page mapping
    void addTupleToPageMap(int tupleId, int pageId) {
        if (tupleToPageMap.find(tupleId) != tupleToPageMap.end()) {
        std::cerr << "Warning addTupleToPageMap: Overwriting existing mapping for Tuple ID " << tupleId << ".\n";
        }
        tupleToPageMap[tupleId] = pageId;

    }

    // Mark a tuple as deleted in the page map
    void removeTupleFromPageMap(int tupleId) {
        tupleToPageMap[tupleId] = -2; // Mark as deleted
    }
    bool hasTupleInPageMap(int tupleID) const {
        auto it = tupleToPageMap.find(tupleID);
        // Return true if tuple exists and is not marked as deleted (-1 or -2)
        return it != tupleToPageMap.end() && it->second != -1 && it->second != -2;
    }

    // Get the schema
    const std::map<std::string, std::string>& getSchema() const {
        return schema;
    }

    // Get the number of pages in the file
    uint16_t getPageCount() const {
        return pageCount;
    }

    // Get the tuple-to-page map
    const std::map<int, int>& getTupleToPageMap() const {
        
        return tupleToPageMap;
    }
    int getPageIDForTuple(int tupleID) const {
    auto it = tupleToPageMap.find(tupleID);
    if (it == tupleToPageMap.end()) return -1; // Tuple does not exist
    if (it->second == -1 || it->second == -2) return -2; // Tuple is deleted
    return it->second; // Return page ID
}


    std::streampos getPagePosition(int pageID) const {
    if (pageID < 0 || pageID > pageCount) {
        std::cerr << "Error getPagePosition: Invalid pageID: " << pageID << " (pageCount: " << pageCount << ")\n";
        throw std::out_of_range("Invalid pageID: " + std::to_string(pageID));
    }
    return std::streampos(METADATA_SIZE+ (pageID * 4096));
}


    void setTupleAsDeleted(int tupleID) {
    tupleToPageMap[tupleID] = -2;
    std::cout << "[DEBUG setTupleAsDeleted] Tuple " << tupleID << " marked as deleted." << std::endl;
}



    bool hasTupleWithID(int tupleID) const {
    auto it = tupleToPageMap.find(tupleID);
    if (it == tupleToPageMap.end()) {
        std::cout << "[DEBUG hasTupleWithID] Tuple " << tupleID << " not found in the map." << std::endl;
        return false;
    }
    if (it->second == -1 || it->second == -2) {
        std::cout << "[DEBUG hasTupleWithID] Tuple " << tupleID << " is marked as deleted." << std::endl;
        return false;
    }
    return true;
}


    // FileMetadata class
void serialize(std::fstream& dbFile, const std::string& filePath) {
    if (!dbFile.is_open() || !dbFile) {
        // Attempt to reopen the file in read-write binary mode
        dbFile.open(filePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }
    }

    try {
        // Serialize the schema
        uint16_t schemaSize = schema.size();
        dbFile.write(reinterpret_cast<char*>(&schemaSize), sizeof(schemaSize));
        for (const auto& [key, value] : schema) {
            uint16_t keySize = key.size();
            uint16_t valueSize = value.size();
            dbFile.write(reinterpret_cast<const char*>(&keySize), sizeof(keySize));
            dbFile.write(key.c_str(), keySize);
            dbFile.write(reinterpret_cast<const char*>(&valueSize), sizeof(valueSize));
            dbFile.write(value.c_str(), valueSize);
        }

        // Serialize page count
        dbFile.write(reinterpret_cast<char*>(&pageCount), sizeof(pageCount));

        // Serialize reserved space
        dbFile.write(reserved, RESERVED_SIZE);

        // Serialize the tuple-to-page map
        uint16_t mapSize = tupleToPageMap.size();
        dbFile.write(reinterpret_cast<char*>(&mapSize), sizeof(mapSize));
        for (const auto& [tupleId, pageId] : tupleToPageMap) {
            dbFile.write(reinterpret_cast<const char*>(&tupleId), sizeof(tupleId));
            dbFile.write(reinterpret_cast<const char*>(&pageId), sizeof(pageId));
        }

        std::cout << "[DEBUG File Metadata serialize] FileMetadata serialized successfully.\n";

    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Serialization failed: ") + e.what());
    }
}
    // Deserialize the metadata from a file
void deserialize(std::fstream& file) {
    if (!file.is_open() || !file) {
        throw std::runtime_error("Error File Metadata deserialize : File stream is not open or valid during deserialization.");
    }

    try {
        // Deserialize schema
        uint16_t schemaSize;
        file.read(reinterpret_cast<char*>(&schemaSize), sizeof(schemaSize));
        schema.clear();
        for (uint16_t i = 0; i < schemaSize; ++i) {
            uint16_t keySize, valueSize;
            file.read(reinterpret_cast<char*>(&keySize), sizeof(keySize));
            std::string key(keySize, '\0');
            file.read(&key[0], keySize);

            file.read(reinterpret_cast<char*>(&valueSize), sizeof(valueSize));
            std::string value(valueSize, '\0');
            file.read(&value[0], valueSize);

            schema[key] = value;
        }

        // Deserialize page count
        file.read(reinterpret_cast<char*>(&pageCount), sizeof(pageCount));
        if (pageCount == 0) {
        //std::cerr << "Warning File Metadata deserialize: Page count is 0, initializing to 1.\n";
        //pageCount = 1;
        }
        // Deserialize reserved space
        file.read(reserved, RESERVED_SIZE);

        // Deserialize tuple-to-page map
        uint16_t mapSize;
        file.read(reinterpret_cast<char*>(&mapSize), sizeof(mapSize));
        tupleToPageMap.clear();
        for (uint16_t i = 0; i < mapSize; ++i) {
            int tupleId, pageId;
            file.read(reinterpret_cast<char*>(&tupleId), sizeof(tupleId));
            file.read(reinterpret_cast<char*>(&pageId), sizeof(pageId));
            tupleToPageMap[tupleId] = pageId;
        }

        std::cout << "[DEBUG File Metadata deserialize] FileMetadata deserialized successfully.\n";

    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("File Metadata Deserialization failed: ") + e.what());
    }
}

    void printMetadata() const {
    std::cout << "=== File Metadata ===\n";

    // Print schema
    std::cout << "Schema:\n";
    if (schema.empty()) {
        std::cout << "  (Schema is empty)\n";
    } else {
        for (const auto& [key, value] : schema) {
            std::cout << "  " << key << ": " << value << "\n";
        }
    }

    // Print page count
    std::cout << "Page Count: " << pageCount << "\n";

    // Print reserved space size
    std::cout << "Reserved Space: " << RESERVED_SIZE << " bytes\n";

    // Print tuple-to-page map
    std::cout << "Tuple-to-Page Map:\n";
    if (tupleToPageMap.empty()) {
        std::cout << "  (Map is empty)\n";
    } else {
        for (const auto& [tupleId, pageId] : tupleToPageMap) {
            std::cout << "  Tuple ID: " << tupleId
                      << ", Page ID: " << (pageId == -1 ? "(Deleted)" : std::to_string(pageId)) << "\n";
        }
    }

    std::cout << "=====================\n";
}


};

// Page represents a logical page within a database file
struct PageMetadata {
    uint16_t pageID;        // Unique page identifier
    uint16_t slotCount;     // Number of active slots
    uint16_t freeSpace;     // Remaining free space in bytes
    uint16_t freeSpaceEnd;  // Offset where free space ends (starting from the back)
};

class Page {
private:

    PageMetadata metadata;        // Stores key page metadata
    std::vector<Slot> slots;      // Slotted array to store tuple metadata
    char data[PAGE_SIZE];
    
public:

    Page(uint16_t id) {
    metadata.pageID = id;
    metadata.slotCount = 0;
    metadata.freeSpace = PAGE_SIZE - sizeof(PageMetadata);
    metadata.freeSpaceEnd = PAGE_SIZE;

    std::memset(data, 0, PAGE_SIZE);  // Clear memory
    }

    uint32_t getPageID() const {
        return metadata.pageID;
    }

    size_t getFreeSpace() const {
        return metadata.freeSpace;
    }
    
    uint16_t getTupleCount() const {
        return metadata.slotCount;  // Return the number of slots/tuples
    }
    // Getter for slots
    const std::vector<Slot>& getSlots() const {
        return slots;
    }
    Slot getSlot(size_t index) const {
        if (index < slots.size()) {
            return slots[index];
        }
        throw std::out_of_range("Slot index out of range");
    }
    bool addTuple(const std::string& tuple, FileMetadata& fileMetadata, int tupleId) {
        std::cout << "Debug addTuple: Attempting to add tuple. Free space: " << metadata.freeSpace
                << ", Tuple size: " << tuple.size() + sizeof(Slot) << std::endl;

        // Check if there's enough space for the tuple and slot metadata
        if (metadata.freeSpace < tuple.size() + sizeof(Slot)) {
            std::cout << "Debug addTuple: Not enough space to add tuple.\n";
            return false; // Not enough space
        }

        // Calculate the offset where the tuple will be placed
        uint16_t tupleOffset = metadata.freeSpaceEnd - tuple.size();
        // Debug: Log tuple offset calculation
        std::cout << "Debug addTuple: Calculating tuple offset. Free space end: " << metadata.freeSpaceEnd
              << ", Tuple size: " << tuple.size() << std::endl;


        // Safety check to prevent writing out of bounds
        if (tupleOffset < sizeof(PageMetadata) + slots.size() * sizeof(Slot)) {
            std::cerr << "Error addTuple: Not enough space for tuple and slot metadata.\n";
            return false;
        }

        // Insert the tuple into the page's data array
        std::memcpy(data + tupleOffset, tuple.c_str(), tuple.size());
        std::cout << "Debug addTuple: Tuple added at offset: " << tupleOffset << " with size: " << tuple.size() << std::endl;


        // Create a new slot for the tuple
        Slot slot = {tupleOffset, static_cast<uint16_t>(tuple.size())};
        slots.push_back(slot);

        // Update page metadata
        metadata.freeSpaceEnd = tupleOffset;
        metadata.freeSpace -= (tuple.size() + sizeof(Slot));
        metadata.slotCount++;
        std::cout << "Debug addTuple: Slot count after adding tuple: " << metadata.slotCount << std::endl;


        // Update FileMetadata with the new tuple location
        fileMetadata.addTupleToPageMap(tupleId, metadata.pageID);
        std::cout << "Debug addTuple: Adding tuple " << tupleId << " to page " << metadata.pageID << std::endl;


        std::cout << "Debug addTuple: Added tuple. Free space left: " << metadata.freeSpace
                << ", Slot count: " << metadata.slotCount << std::endl;

        return true;
    }

    void serialize(std::fstream& dbFile) {
        if (!dbFile) {
            std::cerr << "Error page serialize: File stream is not open or valid.\n";
            return;
        }

        // Write the page metadata
        dbFile.write(reinterpret_cast<char*>(&metadata), sizeof(PageMetadata));
        if (!dbFile) {
            std::cerr << "Error  page serialize: Failed to write page metadata.\n";
            return;
        }
        std::cout << "Debug  page serialize: Serialized page metadata (PageID: " << metadata.pageID << ", SlotCount: " << metadata.slotCount << ")\n";

        // Count and write the number of active slots
        uint16_t activeSlotCount = 0;
        for (const auto& slot : slots) {
            if (slot.length > 0) {
                ++activeSlotCount;
            }
        }
        dbFile.write(reinterpret_cast<char*>(&activeSlotCount), sizeof(activeSlotCount));
        if (!dbFile) {
            std::cerr << "Error  page serialize: Failed to write active slot count.\n";
            return;
        }

        std::cout << "Debug  page serialize: Active slots count: " << activeSlotCount << std::endl;

        // Serialize each active slot
        for (const auto& slot : slots) {
            if (slot.length > 0) {
                dbFile.write(reinterpret_cast<const char*>(&slot), sizeof(Slot));
                if (!dbFile) {
                    std::cerr << "Error  page serialize: Failed to write slot metadata.\n";
                    return;
                }
                std::cout << "Debug  page serialize: Serialized Slot. Offset: " << slot.offset 
                        << ", Length: " << slot.length << std::endl;
            }
        }

        // Write the page data
        dbFile.write(data, PAGE_SIZE);
        if (!dbFile) {
            std::cerr << "Error  page serialize: Failed to write page data.\n";
            return;
        }
        std::cout << "Debug  page serialize: Finished serializing page.\n";
}

void deserialize(std::istream& dbFile) {
    std::cout << "Debug: Deserializing page.\n";

    if (!dbFile) {
        std::cerr << "Error page deserialize: File stream is not open or valid.\n";
        return;
    }

    // Read the page metadata
    dbFile.read(reinterpret_cast<char*>(&metadata), sizeof(PageMetadata));
    if (!dbFile) {
        std::cerr << "Error page deserialize: Failed to read page metadata.\n";
        return;
    }
    std::cout << "Debug page deserialize: Deserialized page metadata successfully. PageID: " << metadata.pageID
              << ", SlotCount: " << metadata.slotCount << "\n";

    // Read the number of slots
    uint16_t slotCount;
    dbFile.read(reinterpret_cast<char*>(&slotCount), sizeof(slotCount));
    if (!dbFile) {
        std::cerr << "Error page deserialize: Failed to read slot count.\n";
        return;
    }
    std::cout << "Debug page deserialize: Slot count from file: " << slotCount << std::endl;

    // Clear existing slots and prepare to load new ones
    slots.clear();
    for (int i = 0; i < slotCount; ++i) {
        Slot slot;
        dbFile.read(reinterpret_cast<char*>(&slot), sizeof(Slot));

         // Debugging output for each slot
        if (dbFile) {
            std::cout << "Deserialized Slot: Offset = " << slot.offset << ", Length = " << slot.length << std::endl;
        } else {
            std::cerr << "Error page deserialize: Failed to read slot " << i << std::endl;
            return;
        }

        // Ensure that the slot has a valid length and does not exceed the page size
        if (slot.length > 0 && slot.offset + slot.length <= PAGE_SIZE) {
            slots.push_back(slot);
            std::cout << "Debug page deserialize: Slot added. Offset: " << slot.offset 
                      << ", Length: " << slot.length << std::endl;
        } else {
            // Log error if the slot is invalid
            std::cerr << "Error page deserialize: Invalid slot at index " << i << ". Offset: " << slot.offset
                      << ", Length: " << slot.length << std::endl;
        }
    }

    // Read the page data
    dbFile.read(data, PAGE_SIZE);
    if (!dbFile) {
        std::cerr << "Error page deserialize: Failed to read page data.\n";
        return;
    }

    std::cout << "Debug page deserialize: Finished deserializing page. PageID: " << metadata.pageID << "\n";

}


std::string getTupleIndex(const std::string& tablePath, uint16_t tupleID) {
    std::fstream dbFile;
    dbFile.open(tablePath, std::ios::in | std::ios::binary);
    if (!dbFile.is_open()) {
        std::cerr << "Error getTupleIndex: Unable to open file: " << tablePath << std::endl;
        return "";
    }

    // Deserialize file metadata (assumed at the beginning of the file)
    FileMetadata fileMetadata;
    fileMetadata.deserialize(dbFile);

     // Check for deserialization success
    if (dbFile.fail()) {
        std::cerr << "Error getTupleIndex: Failed to deserialize file metadata.\n";
        dbFile.close();
        return "";
    }

    // Use the tuple-to-page map to find the page ID associated with the tupleID
    auto it = fileMetadata.getTupleToPageMap().find(tupleID);
    if (it == fileMetadata.getTupleToPageMap().end() || it->second == -2) {
        std::cerr << "Error getTupleIndex: Tuple ID not found or marked as deleted." << std::endl;
        dbFile.close();
        return "";
    }

    uint16_t pageID = it->second;
    std::cout << "Debug getTupleIndex: Found tuple with ID " << tupleID << " on page " << pageID << std::endl;

    // Use your getPagePosition function to get the page position
    uint64_t pagePosition = fileMetadata.getPagePosition(pageID); // Assuming getPagePosition handles the offset correctly
    std::cout << "Debug getTupleIndex: Seeking to page position " << pagePosition << std::endl;

    // Deserialize the page
    Page page(pageID);
    page.deserialize(dbFile);
    // Check for deserialization success
    if (dbFile.fail()) {
        std::cerr << "Error getTupleIndex: Failed to deserialize page " << pageID << ".\n";
        dbFile.close();
        return "";
    }

    // Map the tupleID to the correct slot index
    uint16_t slotIndex = -1;
    for (uint16_t i = 0; i < page.getTupleCount(); ++i) {
        // Assuming the tuple ID is stored in the tuple itself, you could compare here
        std::string tupleData = page.getTupleData(i);  // Retrieve the tuple data
        Tuple tuple;
        std::cout << "Debug getTupleIndex: Deserialized tuple ID: " << tuple.getAttributeValue("id") << std::endl;

        if (tuple.deserialize(tupleData)) {
            if (tuple.getAttributeValue("id") == std::to_string(tupleID)) {
                slotIndex = i;
                std::cout << "Debug getTupleIndex: Found matching tuple with ID " << tupleID << " at slot index " << i << std::endl;

                break;  // Found the tuple with the matching ID
            }
        }
    }

    if (slotIndex == -1) {
        std::cerr << "Error getTupleIndex: Tuple ID not found on the page." << std::endl;
        dbFile.close();
        return "";
    }

    // Now, retrieve the tuple data from the page using the found slotIndex
    std::string tupleData = page.getTupleData(slotIndex);
    std::cout << "Debug getTupleIndex: Retrieved tuple data: " << tupleData << std::endl;

    dbFile.close();
    return tupleData;
}


    std::string getTupleData(uint16_t index) const {
         // Debug: Check if the index is valid
        std::cout << "Debug getTupleData: Retrieving tuple at index " << index << std::endl;

        if (index >= slots.size() || slots[index].length == 0) {
            std::cerr << "Error getTupleData: Tuple ID not found at index " << index << std::endl;
            throw std::out_of_range("Tuple ID not found");
        }
        if (slots[index].offset + slots[index].length > PAGE_SIZE) {
        std::cerr << "Error getTupleData: Corrupted page data. Tuple offset and length are out of bounds." << std::endl;
        throw std::runtime_error("Corrupted page data.");
    }
        const Slot& slot = slots[index];
        std::cout << "Debug getTupleData: Tuple found. Offset: " << slot.offset << ", Length: " << slot.length << std::endl;

        return std::string(data + slot.offset, slot.length);
    }
    

bool deleteTuple(uint16_t slotIndex, int tupleID, const std::string& tablePath) {
    std::cout << "Debug deleteTuple: Attempting to delete tuple with ID " << tupleID << " at slot index " << slotIndex << std::endl;

    if (slotIndex >= slots.size() || slots[slotIndex].length == 0) {
        std::cerr << "Error deleteTuple: Tuple not found or already deleted at slot index " << slotIndex << std::endl;
        return false;  // Tuple not found or already deleted
    }
    
    Slot& slot = slots[slotIndex];
    
    // Open the file to update the tuple-to-page map
    std::fstream dbFile(tablePath, std::ios::in | std::ios::out | std::ios::binary);
    if (!dbFile.is_open()) {
        std::cerr << "Error deleteTuple: Unable to open file: " << tablePath << std::endl;
        return false;
    }
    
    // Deserialize the file metadata
    FileMetadata fileMetadata;
    fileMetadata.deserialize(dbFile);
    
    // Remove the tuple from the page map (mark as deleted)
    fileMetadata.removeTupleFromPageMap(tupleID);
    std::cout << "Debug deleteTuple: Tuple with ID " << tupleID << " is marked as deleted in the page map." << std::endl;
    
    // Clear the data associated with the slot
    std::memset(data + slot.offset, 0, slot.length);
    std::cout << "Debug deleteTuple: Cleared data at offset " << slot.offset << ", Length: " << slot.length << std::endl;

    // Reset the slot metadata to mark the tuple as deleted
    slot.length = 0;
    slot.offset = 0;
    metadata.slotCount--;
    std::cout << "Debug deleteTuple: Slot marked as deleted. Remaining slot count: " << metadata.slotCount << std::endl;

    
    // Move the file pointer back to the beginning of the file before writing the updated metadata
    dbFile.seekp(0, std::ios::beg);
    
    // Serialize the updated file metadata
    fileMetadata.serialize(dbFile,tablePath);  // This will update the metadata in the file
    std::cout << "Debug deleteTuple: Updated file metadata written to the file." << std::endl;

    dbFile.close();
    return true;
}

    int getTupleIndexByID(const std::string& id) const {
        std::cout << "Debug getTupleIndexByID: Searching for tuple with ID " << id << std::endl;

    // Iterate over all slots to find the tuple with the matching ID
    for (size_t i = 0; i < slots.size(); ++i) {
        if (slots[i].length == 0) {
            continue;  // Skip empty slots
        }

        // Extract tuple data from the page
        std::string tupleData(data + slots[i].offset, slots[i].length);

        // Deserialize the tuple
        Tuple tuple;
        if (tuple.deserialize(tupleData)) {
            std::string tupleID = tuple.getAttributeValue("id");
            std::cout << "Debug getTupleIndexByID: Checking tuple ID " << tupleID << " at slot " << i << std::endl;

            // Compare the deserialized ID with the requested ID
            if (tupleID == id) {
                std::cout << "Debug getTupleIndexByID: Found tuple with ID " << id << " at slot index " << i << std::endl;
                return i;  // Return the index of the found tuple
            }
        }
    }

   
    std::cout << "Debug getTupleIndexByID: Tuple with ID " << id << " not found." << std::endl;
    // If not found, return -1
    return -1;
}
};

class Storage {

    private:
    std::vector<Page> pages;
    uint32_t nextPageID = 1; // Unique page ID counter

    private:
    std::map<std::string, std::map<std::string, std::map<std::string, Tuple>>> databases;

    public:
    bool createDatabase(const std::string& dbName) {
        if (!fs::exists(dbName)) {
            if (fs::create_directory(dbName)) {
                std::cout << "Database folder created: " << dbName << std::endl;
            } else {
                std::cerr << "Error: Database could not be created.\n";
                return false;
            }
        }
        return true;
    }

    bool tableExists(const std::string& dbName, const std::string& tableName) {
        std::string tablePath = dbName + "/" + tableName + ".HAD";
        return fs::exists(tablePath);
    }

    bool createTable(const std::string& dbName, const std::string& tableName, const std::map<std::string, std::string>& schema) {
    std::string tablePath = dbName + "/" + tableName + ".HAD";
    std::cout << "Debug createTable: Creating table at path: " << tablePath << std::endl;

    // Check if the table file already exists
    if (fs::exists(tablePath)) {
        std::cout << "Table already exists: " << tablePath << std::endl;
        return true; // Table exists, so continue
    }

    // Create a new table file
    std::fstream newTable(tablePath, std::ios::binary | std::ios::out | std::ios::in | std::ios::trunc);
    if (newTable) {
        // Initialize metadata
        FileMetadata metadata;
        metadata.setPageCount(0); // Start with 0 pages
        metadata.setSchema(schema); // Use the provided schema
        std::cout << "Debug createTable: Initialized metadata with 0 pages and provided schema." << std::endl;

        
        // Serialize metadata to the file
        metadata.serialize(newTable,tablePath);  // This will write metadata
        if (newTable) {
            std::cout << "Debug createTable: Serialized metadata to table file successfully." << std::endl;
        } else {
            std::cerr << "Error createTable: Failed to write metadata to table file." << std::endl;
        }
        std::cout << "Size of FileMetadata: " << sizeof(metadata) << " bytes" << std::endl;
        newTable.close();
        std::cout << "Created new table with metadata: " << tablePath << std::endl;
        return true;
    }

    std::cerr << "Error createTable: Failed to create table file at path: " << tablePath << std::endl;
    return false;
}

// Function to delete a table from the database
bool deleteTable(const std::string& tablePath) {
    std::cout << "Debug deleteTable: Attempting to delete table at path: " << tablePath << std::endl;

    if (fs::exists(tablePath)) {
        std::cout << "Debug deleteTable: Table found, proceeding to delete..." << std::endl;

        try {
            fs::remove(tablePath); // Remove the table file
           std::cout << "Debug deleteTable: Table deleted successfully: " << tablePath << std::endl;
            return true;
        } catch (const fs::filesystem_error& e) {
            std::cerr << "Error deleting table: " << e.what() << std::endl;
        }
    } else {
        std::cerr << "Error deleteTable: Table not found: " << tablePath << std::endl;
    }
    return false;
}

// Helper function to load a page from the table
Page loadPageByID(const std::string& tablePath, uint32_t pageID) {
    std::cout << "Debug loadPageByID: Attempting to load page with ID: " << pageID << " from table: " << tablePath << std::endl;

    std::fstream dbFile(tablePath, std::ios::in | std::ios::binary);
    if (!dbFile.is_open()) {
        throw std::runtime_error("Failed to open table file: " + tablePath);
    }
    std::cout << "Debug loadPageByID: Successfully opened table file." << std::endl;

    // Read file metadata
    FileMetadata fileMetadata;
    fileMetadata.deserialize(dbFile);
    std::cout << "Debug loadPageByID: File metadata deserialized successfully." << std::endl;

    uint32_t pagePosition = fileMetadata.getPagePosition(pageID);
    std::cout << "Debug loadPageByID: Calculated page position for page ID " << pageID << ": " << pagePosition << std::endl;

    dbFile.seekg(pagePosition, std::ios::beg);
    if (!dbFile) {
        throw std::runtime_error("Failed to seek to page position: " + std::to_string(pagePosition));
    }
    std::cout << "Debug loadPageByID: Seeked to page position successfully." << std::endl;

    // Load the page from the file
    Page page(pageID);
    page.deserialize(dbFile);
    std::cout << "Debug loadPageByID: Page with ID " << pageID << " loaded successfully." << std::endl;

    dbFile.close();
    return page;
}

std::vector<Tuple> getTuplesFromPage(const Page& page) {
    std::vector<Tuple> tuples;
    std::cout << "Debug getTuplesFromPage: Retrieving tuples from page. Total slot count: " << page.getTupleCount() << std::endl;

    // Iterate through the slots in the page
    for (size_t i = 0; i < page.getTupleCount(); ++i) {
        try {
            // Retrieve tuple data from the page using the slot index
            std::string tupleData = page.getTupleData(i);
            std::cout << "Debug getTuplesFromPage: Retrieved tuple data from slot " << i << ". Data length: " << tupleData.size() << std::endl;

            // Deserialize the tuple data into a Tuple object
            Tuple tuple;
           if (tuple.deserialize(tupleData)) {
                tuples.push_back(tuple); // Add the tuple to the result
                std::cout << "Debug getTuplesFromPage: Tuple deserialized successfully at slot " << i << std::endl;
            } else {
                std::cerr << "Error getTuplesFromPage: Failed to deserialize tuple data at slot " << i << std::endl;
            }
  
        } catch (const std::exception& e) {
            std::cerr << "Error retrieving tuple at slot " << i << ": " << e.what() << std::endl;
        }
    }
    std::cout << "Debug getTuplesFromPage: Retrieved " << tuples.size() << " tuples from the page." << std::endl;

    return tuples;
}

    // Check if a tuple with a specific ID exists in a file
bool hasTupleWithIDInFile(const std::string& tablePath, int id) {
    // Open the table file in binary read mode
    std::fstream dbFile(tablePath,std::ios::in | std::ios::out | std::ios::binary);
    if (!dbFile.is_open()) {
        std::cerr << "Failed to open table file: " << tablePath << "\n";
        return false;
    }

    // Deserialize file metadata from the beginning of the file
    FileMetadata fileMetadata;
    try {
        fileMetadata.deserialize(dbFile);
        std::cout << "Debug hasTupleWithIDInFile: Successfully deserialized file metadata.\n";
    } catch (const std::exception& e) {
        std::cerr << "Error during file metadata deserialization: " << e.what() << "\n";
        dbFile.close();
        return false;
    }

    // Use the tuple-to-page map to find the page ID associated with the tuple ID
    const auto& tupleToPageMap = fileMetadata.getTupleToPageMap();
    std::cout << "Debug hasTupleWithIDInFile: Checking for tuple with ID " << id << " in the tuple-to-page map.\n";

    auto it = tupleToPageMap.find(id);

    if (it == tupleToPageMap.end()) {
        std::cerr << "Error hasTupleWithIDInFile: Tuple ID " << id << " not found in the tuple-to-page map.\n";
        dbFile.close();
        return false;
    }
     // Check if the tuple is marked as deleted (e.g., -2 for deletion)
    if (it->second == -2) {
        std::cout << "Debug hasTupleWithIDInFile: Tuple ID " << id << " is marked as deleted.\n";
        dbFile.close();
        return false;
    }
    // If we found the tuple ID in the map and it's not deleted, return true
    std::cout << "Debug hasTupleWithIDInFile: Tuple with ID " << id << " found and not deleted.\n";
    dbFile.close();
    return true;
}

std::string loadTuple(const std::string& tablePath, uint16_t tupleID) {
    // Open the database file in read-binary mode
    std::fstream dbFile(tablePath, std::ios::in | std::ios::binary);
    if (!dbFile.is_open()) {
        std::cerr << "Error loadTuple: Unable to open file: " << tablePath << std::endl;
        return "";
    }

    // Deserialize the file metadata
    FileMetadata fileMetadata;
    try {
        fileMetadata.deserialize(dbFile);
        std::cout << "Debug loadTuple : Successfully deserialized file metadata." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error loadTuple: Failed to deserialize file metadata: " << e.what() << std::endl;
        dbFile.close();
        return "";
    }

    // Use the tuple-to-page map to find the page ID associated with the tupleID
    auto it = fileMetadata.getTupleToPageMap().find(tupleID);
    if (it == fileMetadata.getTupleToPageMap().end() || it->second == -2) {
        std::cerr << "Error loadTuple: Tuple ID " << tupleID << " not found or marked as deleted." << std::endl;
        dbFile.close();
        return "";
    }

    uint16_t pageID = it->second;
    std::cout << "Debug loadTuple: Found tuple with ID " << tupleID << " on page " << pageID << std::endl;

    // Seek to the page position based on the pageID
    uint32_t pagePosition = fileMetadata.getPagePosition(pageID);
    dbFile.seekg(pagePosition, std::ios::beg);
    if (!dbFile.good()) {
        std::cerr << "Error loadTuple: Failed to seek to page position " << pagePosition << std::endl;
        dbFile.close();
        return "";
    }

    // Deserialize the page
    Page page(pageID);
    try {
        page.deserialize(dbFile);
        std::cout << "Debug loadTuple: Page with ID " << pageID << " deserialized successfully." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error loadTuple: Failed to deserialize page: " << e.what() << std::endl;
        dbFile.close();
        return "";
    }

    // Get the tuple data using the tuple index (this assumes the tuple ID is unique per page)
    int slotIndex = page.getTupleIndexByID(std::to_string(tupleID));
    if (slotIndex == -1) {
        std::cerr << "Error loadTuple: Tuple ID " << tupleID << " not found on the page." << std::endl;
        dbFile.close();
        return "";
    }

    // Retrieve the tuple data from the page using the slot index
    std::string tupleData = page.getTupleData(slotIndex);
    std::cout << "Debug loadTuple: Tuple with ID " << tupleID << " retrieved successfully." << std::endl;

    dbFile.close();
    return tupleData;
}


std::map<std::string, std::string> get(const std::string& dbName, const std::string& tableName, const std::string& id) {
    // Construct the table path
    std::string tablePath = dbName + "/" + tableName + ".HAD";

    // Open the table file
    std::fstream file(tablePath, std::ios::binary | std::ios::in);
    if (!file) {
        throw std::runtime_error("Failed to open the table file.");
    }

    // Read file metadata to get the tuple-to-page map
    FileMetadata fileMetadata;
    try {
        fileMetadata.deserialize(file);
    } catch (const std::exception& e) {
        file.close();
        throw std::runtime_error("Error deserializing file metadata: " + std::string(e.what()));
    }
    
    uint32_t tupleId;
    try {
        tupleId = std::stoi(id);  // Convert string id to integer
    } catch (const std::invalid_argument& e) {
        file.close();
        throw std::invalid_argument("Invalid ID format: " + id);
    }

    // Check if the tuple exists in the map
    auto it = fileMetadata.getTupleToPageMap().find(stoi(id));
    if (it == fileMetadata.getTupleToPageMap().end() || it->second == -1) {
        // Tuple ID not found or is marked as deleted
        file.close();
        throw std::out_of_range("Tuple ID not found");
    }

    // Get the page ID from the map
    uint32_t pageID = it->second;

    // Calculate the position of the page in the file
    file.seekg(fileMetadata.getPagePosition(pageID), std::ios::beg);

     if (!file.good()) {
        file.close();
        throw std::runtime_error("Failed to seek to page position in file.");
    }

    // Load the page
    Page page(pageID);
    try {
        page.deserialize(file);
    } catch (const std::exception& e) {
        file.close();
        throw std::runtime_error("Error deserializing page with ID " + std::to_string(pageID) + ": " + std::string(e.what()));
    }

    // Search for the tuple in the page
    for (uint16_t i = 0; i < page.getTupleCount(); ++i) {
        std::string tupleData = page.getTupleData(i);
        Tuple tuple;

        // Deserialize the tuple and check if the ID matches
        if (tuple.deserialize(tupleData) && tuple.getAttributeValue("id") == id) {
            file.close();

            // Create a map to store the tuple's key-value pairs
            std::map<std::string, std::string> result;
            for (const auto& attribute : tuple.getAttributes()) {
                // Assuming tuple.getAttributes() returns a list of attributes as pairs of <key, value>
                result[attribute.first] = attribute.second.second;
            }
            return result; // Return the map of key-value pairs
        }
    }

    // If the tuple was not found
    file.close();
    throw std::out_of_range("Tuple with ID " + id + " not found on page " + std::to_string(pageID));

}

    // Add a tuple to the table, ensuring ID uniqueness across the entire file
bool addTupleToTable(const std::string& dbName, const std::string& tableName, const std::string& tupleSerialized, int id) {
    std::string tablePath = dbName + "/" + tableName + ".HAD";
    std::cout << "Debug addTupleToTable: Adding tuple to table file: " << tablePath << std::endl;

    // Check if the table exists
    if (!fs::exists(tablePath)) {
        std::cerr << "Table does not exist: " << tablePath << std::endl;
        return false;
    }

    // // Deserialize the tuple to extract the ID
    // Tuple tuple;
    // if (!tuple.deserialize(tupleSerialized)) {
    //     std::cerr << "Failed to deserialize the tuple.\n";
    //     return false;
    // }

    // std::string id = tuple.getAttributeValue("id");
    // std::cout << "Debug addTupleToTable: Deserialized tuple with ID: " << id << std::endl;
    // if (id.empty()) {
    //     std::cerr << "Tuple must have a valid 'id' attribute.\n";
    //     return false;
    // }

    //  try {
    //     int tupleID = std::stoi(id);  // Convert string id to integer
    //     if (hasTupleWithIDInFile(tablePath, tupleID)) {
    //         std::cerr << "Duplicate ID detected (" << id << "). Skipping insertion.\n";
    //         return false;
    //     }
    // } catch (const std::invalid_argument& e) {
    //     std::cerr << "Error addTupleToTable: Invalid ID format (" << id << ").\n";
    //     return false;
    // }

    // Open the table file for reading and writing
    std::fstream file(tablePath, std::ios::binary | std::ios::in | std::ios::out);
    if (!file) {
        std::cerr << "Failed to open table file for reading and writing.\n";
        return false;
    }
    std::cout << "Debug addTupleToTable: Successfully opened table file for reading and writing.\n";

    FileMetadata fileMetadata;
    try {
        fileMetadata.deserialize(file);
        std::cout << "Debug addTupleToTable: Deserialized file metadata.\n";
    } catch (const std::exception& e) {
        std::cerr << "Error deserializing file metadata: " << e.what() << std::endl;
        file.close();
        return false;
    }

    // Check if there is space in the existing pages
    int pageId = fileMetadata.getPageCount(); // Assuming this is the next available page ID
    size_t pagePosition = fileMetadata.getPagePosition(pageId);  // Calculate the position of the page

    // Read the page to check for space
    file.seekg(pagePosition);
    Page page(pageId);
    page.deserialize(file);

    std::cout << "Debug addTupleToTable: Page deserialized.\n";
    // Try to add the tuple to this page
    if (page.addTuple(tupleSerialized, fileMetadata,id)) {
        std::cout << "Debug addTupleToTable: Writing updated page at position " << pagePosition << "\n";
        
        file.seekp(pagePosition);
        page.serialize(file);
         std::cout << "Debug addTupleToTable: Updated page serialized and written to file.\n";
        
        // Update metadata after adding a tuple to an existing page
        file.seekp(0);  // Go to the beginning of the file to write metadata
        fileMetadata.serialize(file,tablePath);  // Update the metadata
        // Flush and close the file
        file.flush();
        file.close();

        std::cout << "Debug addTupleToTable: Tuple successfully added to existing page.\n";
        return true;  // Tuple successfully added
    } else {
        std::cerr << "Error addTupleToTable: Failed to add tuple to page.\n";
    }


    // If no existing page had space, create a new page and append it
    
    Page newPage(fileMetadata.getNextPageID()); // Assuming this method gives the next available page ID
    std::cout << "Debug addTupleToTable: No space on existing pages. Creating a new page with ID: " << newPage.getPageID() << "\n";
    
    if (!newPage.addTuple(tupleSerialized, fileMetadata, id)) {
        std::cerr << "Failed to add tuple to a new page.\n";
        file.close();
        return false;
    }

    // Append the new page to the end of the file
    file.clear();
    file.seekp(0, std::ios::end);
    newPage.serialize(file);
    std::cout << "Debug addTupleToTable: New page serialized and appended to file.\n";

    // Increment the page ID after creating a new page
    fileMetadata.incrementPageID();
    file.seekp(0);  // Seek to the beginning of the file to write metadata
    fileMetadata.serialize(file,tablePath);  // Write updated metadata
    file.close();
    
    std::cout << "Debug addTupleToTable: Tuple successfully added to a new page.\n";;
    return true;
}

bool checkTupleExists(const std::string& dbName, const std::string& tableName, const std::string& id) {
    // Check if the database exists
    if (!fs::exists(dbName)) {
        std::cerr << "Database '" << dbName << "' not found.\n";
        return false;
    }

    // Check if the table exists
    std::string tablePath = dbName + "/" + tableName + ".HAD";
    if (!fs::exists(tablePath)) {
        std::cerr << "Table '" << tableName << "' not found in database '" << dbName << "'.\n";
        return false;
    }

    int tupleID;
    try {
        tupleID = std::stoi(id);  // Convert string id to integer
    } catch (const std::invalid_argument& e) {
        std::cerr << "Invalid ID format: '" << id << "'. ID must be a valid integer.\n";
        return false;
    } catch (const std::out_of_range& e) {
        std::cerr << "ID '" << id << "' is out of range.\n";
        return false;
    }
    // Open the table file for reading
    std::fstream file(tablePath, std::ios::binary | std::ios::in);
    if (!file) {
        std::cerr << "Failed to open table file: " << tablePath << "\n";
        return false;
    }

    // Read the file metadata
    FileMetadata fileMetadata;
    try {
        fileMetadata.deserialize(file);
    } catch (const std::exception& e) {
        std::cerr << "Error deserializing file metadata: " << e.what() << "\n";
        file.close();
        return false;
    }

    // Check if the tuple ID exists in the tuple-to-page map in file metadata
    if (fileMetadata.hasTupleInPageMap(std::stoi(id))) {
        std::cout << "Tuple with ID '" << id << "' found in table: " << tableName << " (via metadata lookup).\n";
        file.close();
        return true; // Tuple found via metadata map
    }

    std::cerr << "Tuple with ID '" << id << "' not found in table: " << tableName << " (via metadata map).\n";
    file.close();
    return false; // Tuple does not exist
}

bool insert(const std::string& dbName, const std::string& tableName, const Tuple& tuple) {

        std::map<int, std::string> typeMap = {
        {1, "int"},
        {2, "string"},
        {3, "double"},
        // Add more types as needed
    };
    // Validate database existence
    if (!fs::exists(dbName)) {
        std::cerr << "Database does not exist: " << dbName << std::endl;
        return false;
    }

    // Validate table existence
    std::string tablePath = dbName + "/" + tableName + ".HAD";
    if (!fs::exists(tablePath)) {
        std::cerr << "Table does not exist: " << tableName << std::endl;
        return false;
    }

    // Open the table file for reading
    std::fstream file(tablePath, std::ios::binary | std::ios::in | std::ios::out);
    if (!file) {
        std::cerr << "Failed to open table file: " << tablePath << "\n";
        return false;
    }

    // Read file metadata, including schema
    FileMetadata fileMetadata;
    fileMetadata.deserialize(file);

    // Extract and validate tuple attributes against schema in file metadata
    std::map<std::string, std::pair<int, std::string>> attributes = tuple.getAttributes();
    for (const auto& [key, type] : fileMetadata.getSchema()) {
        // Check if attribute exists in tuple
        if (attributes.find(key) == attributes.end()) {
            std::cerr << "Missing required attribute: " << key << std::endl;
            return false;
        }

        // Check data type and length
        const auto& [attrType, attrValue] = attributes[key];
        if (typeMap[attrType] != type) {
            std::cerr << "Type mismatch for attribute: " << key << std::endl;
            return false;
        }
    }

    // Check if 'id' is unique using tuple-to-page map in file metadata
    std::string idValue = attributes["id"].second; // Assuming "id" is always present
    int id = std::stoi(idValue);
    if (fileMetadata.hasTupleWithID(id)) {
        std::cerr << "Duplicate ID: " << id << " for table: " << tableName << std::endl;
        return false;
    }

    // Serialize tuple and add to the table
    std::string serializedTuple = tuple.serialize();


    if (!addTupleToTable(dbName, tableName, serializedTuple,id)) {
        std::cerr << "Failed to add tuple to table: " << tableName << std::endl;
        return false;
    }

    std::cout << "Debug insert: Tuple successfully added to table: " << tableName << std::endl;
    file.flush();
    return true;
}



bool deleteTupleFromTable(const std::string& dbName, const std::string& tableName, const std::string& id) {
    std::string tablePath = dbName + "/" + tableName + ".HAD";
    std::cout << "Debug deleteTupleFromTable: Deleting tuple from table file: " << tablePath << std::endl;

    // Check if the table exists
    if (!fs::exists(tablePath)) {
        std::cerr << "Table does not exist: " << tablePath << std::endl;
        return false;
    }

    // Open the table file for reading and writing
    std::fstream file(tablePath, std::ios::binary | std::ios::in | std::ios::out);
    if (!file) {
        std::cerr << "Failed to open table file for reading and writing.\n";
        return false;
    }
    std::cout << "Debug deleteTupleFromTable: Table file opened for reading and writing.\n";


    // Read file metadata (including tuple-to-page map)
    FileMetadata fileMetadata;
    fileMetadata.deserialize(file);
    std::cout << "Debug deleteTupleFromTable: File metadata deserialized.\n";

    // Check if the tuple exists using the tuple-to-page map
    int tupleID = std::stoi(id);
    if (!fileMetadata.hasTupleWithID(tupleID)) {
        std::cerr << "Tuple with ID " << id << " does not exist.\n";
        file.close();
        return false;
    }

    std::cout << "Debug deleteTupleFromTable: Tuple with ID " << id << " found in the file metadata.\n";

    // Retrieve the page ID from the map
    int pageID = fileMetadata.getPageIDForTuple(tupleID);
    std::cout << "Debug deleteTupleFromTable: Found page ID " << pageID << " for tuple ID " << id << ".\n";


    // Locate the corresponding page and find the tuple
    Page page(pageID);
    std::streampos pagePos = file.tellg();
    file.seekg(fileMetadata.getPagePosition(pageID), std::ios::beg); // Move to the correct page position
    page.deserialize(file);

    bool tupleFound = false;
    for (uint16_t i = 0; i < page.getTupleCount(); ++i) {
        std::string tupleData = page.getTupleData(i);
        Tuple tuple;
        if (tuple.deserialize(tupleData) && tuple.getAttributeValue("id") == id) {
            tupleFound = true; // Tuple found, proceed to delete
             std::cout << "Debug deleteTupleFromTable: Tuple with ID " << id << " found in the page.\n";
            if (page.deleteTuple(i, tupleID, tablePath)) { // Call deleteTuple from Page class
                // Update the tuple-to-page map and mark the tuple as deleted
                fileMetadata.setTupleAsDeleted(tupleID);
                std::cout << "Debug deleteTupleFromTable: Tuple marked as deleted in file metadata.\n";

                // Write the modified page back to the file
                file.seekp(pagePos); // Move the write pointer to the start of the page
                page.serialize(file);
                fileMetadata.serialize(file,tablePath); // Re-serialize the metadata
                std::cout << "Debug deleteTupleFromTable: Page and file metadata serialized back to file.\n";

                file.close();
                std::cout << "Successfully deleted tuple with ID: " << id << std::endl;
                return true; // Tuple successfully deleted
            }
        }
    }

    file.close();
    if (!tupleFound) {
        std::cerr << "Failed to delete tuple with ID: " << id << ". It may not exist.\n";
    }
    return false; // Tuple with the given ID was not found
}
bool updateTupleInTable(const std::string& dbName, const std::string& tableName, const std::string& id, const Tuple& updatedTuple) {
    std::string tablePath = dbName + "/" + tableName + ".HAD";
    std::cout << "Debug: Attempting to update tuple in table file: " << tablePath << std::endl;

    // Check if the table exists
    if (!fs::exists(tablePath)) {
        std::cerr << "Table does not exist: " << tablePath << std::endl;
        return false;
    }
    std::cout << "Debug: Table exists: " << tablePath << std::endl;

    // Check if the tuple exists using the tuple-to-page map in Storage class
    std::cout << "Debug: Checking if the tuple with ID " << id << " exists in the table." << std::endl;
    if (!deleteTupleFromTable(dbName, tableName, id)) {
        std::cerr << "Failed to delete the tuple with ID " << id << std::endl;
        return false; // Exit if the tuple could not be deleted
    }
    std::cout << "Debug: Tuple with ID " << id << " marked for deletion.\n";

    // Now that the old tuple is deleted, insert the updated tuple into the table
    std::cout << "Debug: Attempting to insert the updated tuple." << std::endl;
    if (!insert(dbName, tableName, updatedTuple)) {
        std::cerr << "Failed to insert the updated tuple.\n";
        return false; // Exit if the updated tuple could not be inserted
    }
    std::cout << "Debug: Updated tuple inserted successfully into the table.\n";

    std::cout << "Successfully updated tuple with ID: " << id << std::endl;
    return true; // Tuple successfully updated
}

};


int main() {
    // Create a Storage object to manage databases
    Storage storage;
    
    // Define the database and table names
    std::string dbName = "testDB";
    std::string tableName = "users";
    
    // Define a simple schema for the table
    std::map<std::string, std::string> schema = {
        {"id", "int"},
        {"name", "string"},
        {"age", "int"}
    };
    
    // Step 1: Create the database if it doesn't exist
    if (!storage.createDatabase(dbName)) {
        std::cerr << "Failed to create the database." << std::endl;
        return -1;
    }

    // Step 2: Create the table with the schema
    if (!storage.createTable(dbName, tableName, schema)) {
        std::cerr << "Failed to create the table." << std::endl;
        return -1;
    }

    // Step 3: Prepare a tuple to insert into the table
    Tuple newTuple;
    newTuple.addAttribute("id", 1, "1");       // ID attribute, value = 1
    newTuple.addAttribute("name", 2, "Alice");  // Name attribute, value = "Alice"
    newTuple.addAttribute("age", 1, "30");      // Age attribute, value = 30
    
    // Step 4: Try to insert the tuple into the table
    if (storage.insert(dbName, tableName, newTuple)) {
        std::cout << "Tuple inserted successfully!" << std::endl;
    } else {
        std::cerr << "Failed to insert the tuple." << std::endl;
        return -1;
    }

    // Step 5: Verify that the tuple is inserted
    std::string idValue = newTuple.getAttributeValue("id");
    std::cout << "Inserted tuple ID: " << idValue << std::endl;
    
    // Additional checks can be performed to ensure the data is correctly inserted.

    return 0;
}
