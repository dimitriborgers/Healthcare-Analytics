import requests,json
from abc import ABC,abstractmethod
#----------------------------------------#
#                  Base                  #
#----------------------------------------#
class DataMergerBase(ABC):

    def __init__(self,inputFile,outputFile):

        #Open json file as read-only
        with open(inputFile) as f:
            self.data = json.load(f)

        #Open output file as write-only
        self.output = open(outputFile,'w')

        #Counter
        self.count = 0

    @abstractmethod
    def mergeSets(self,other):
        pass
#----------------------------------------#
#               Sinai Imp.               #
#----------------------------------------#
class SinaiMerger(DataMergerBase):

    def __init__(self,inputFile,outputFile):
        super().__init__(inputFile,outputFile)

    def mergeSets(self,start=0,stop=6000):

        #Create lasting TCP connection
        with requests.Session() as s:

            #Create json container
            jsonContainer = {}

            #Loop through each element in self.data
            for provider in self.data:

                #Loop through range
                if start < stop:

                    #Acquire variables used for payloads
                    try:
                        lastName = self.data[provider]['lastName']
                        firstName = self.data[provider]['firstName']
                        city = self.data[provider]['patientOffices'][0]['city']
                        state = self.data[provider]['patientOffices'][0]['state']
                    except:
                        city = state = None
                    finally:
                        print(firstName+' '+lastName)

                    try:
                        specialty = self.data[provider]['primarySpecialty']['name']
                    except:
                        specialty = None

                    #Check how many results are provided in response
                    payload = {'last_name':lastName,'first_name':firstName,'limit':200}
                    responseFirstLast = s.get('https://npiregistry.cms.hhs.gov/api/resultsDemo2/',params=payload).json()

                    #Error prevention-API did not work
                    if 'Errors' in responseFirstLast:
                        start += 1
                        continue

                    #Error prevention-API query did not work
                    try:
                        print(responseFirstLast['result_count'])
                    except:
                        start += 1
                        continue

                    #Go through flow control
                    if responseFirstLast['result_count'] == 1:
                        result = responseFirstLast

                        #Find correct NPI
                        providerNPI = result['results'][0]['number']

                        #Add information to new JSON file
                        jsonContainer[providerNPI] = self.data[provider]
                        jsonContainer[providerNPI]['mergedInfo'] = result
                        self.count += 1

                    elif responseFirstLast['result_count'] > 1:
                        if city and state:
                            payload = {'last_name':lastName,'first_name':firstName,'city':city,'state':state,'limit':200}
                            responseWithLocation = s.get('https://npiregistry.cms.hhs.gov/api/resultsDemo2/',params=payload).json()

                            #Error prevention-Count or non-functioning API
                            if 'Errors' in responseWithLocation:
                                start += 1
                                continue
                            if responseWithLocation['result_count'] > 1 or responseWithLocation['result_count'] == 0:
                                start += 1
                                continue

                            result = responseWithLocation

                            #Find correct NPI
                            providerNPI = result['results'][0]['number']

                            #Add information to new JSON file
                            jsonContainer[providerNPI] = self.data[provider]
                            jsonContainer[providerNPI]['mergedInfo'] = result
                            self.count += 1

                        elif specialty:
                            payload = {'last_name':lastName,'first_name':firstName,'taxonomy_description':specialty,'limit':200}
                            responseWithSpecialty = s.get('https://npiregistry.cms.hhs.gov/api/resultsDemo2/',params=payload).json()

                            #Error prevention
                            if 'Errors' in responseWithSpecialty:
                                start += 1
                                continue
                            if responseWithSpecialty['result_count'] > 1 or responseWithSpecialty['result_count'] == 0:
                                start += 1
                                continue

                            result = responseWithSpecialty

                            #Find correct NPI
                            providerNPI = result['results'][0]['number']

                            #Add information to new JSON file
                            jsonContainer[providerNPI] = self.data[provider]
                            jsonContainer[providerNPI]['mergedInfo'] = result
                            self.count += 1
                        else:
                            start += 1
                            continue

                    elif responseFirstLast['result_count'] < 1:
                        payload = {'last_name':lastName,'limit':200}
                        responseLast = s.get('https://npiregistry.cms.hhs.gov/api/resultsDemo2/',params=payload).json()

                        #Error prevention-No data found
                        if 'Errors' in responseLast:
                            start += 1
                            continue
                        if responseLast['result_count'] == 0:
                            start += 1
                            continue

                        #Create list of result buckets
                        result_list = []
                        result_list.append(responseLast)
                        skip_number = 200

                        #Maximum amount that can be skipped is 1000
                        while responseLast['result_count'] == 200 and skip_number <= 1000:
                            payload = {'last_name':lastName,'limit':200,'skip':skip_number}
                            responseLast = s.get('https://npiregistry.cms.hhs.gov/api/resultsDemo2/',params=payload).json()
                            result_list.append(responseLast)
                            skip_number += 200

                        #Focus API request by first name
                        index = self._helper_loop(result_list,firstName)
                        if index:
                            #Find correct NPI
                            providerNPI = index['number']

                            #Add information to new JSON file
                            jsonContainer[providerNPI] = self.data[provider]
                            jsonContainer[providerNPI]['mergedInfo'] = index
                            self.count += 1

                    #increment counter
                    start += 1
                    
            #Once all json elements have been added, dump into other file
            json.dump(jsonContainer, self.output, indent=2)

            #close outputFile
            self.output.close()

            print('{}%'.format((self.count/start)*100))

    #Assure there isn't a duplicate name
    def _helper_loop(self,result_list,firstName):

        #Create temporary holders
        nameHolder = None
        nameCounter = 0

        #Loop through every element in bucket
        for bucket in result_list:
            for index in bucket['results']:
                if firstName.lower() in index['basic']['first_name'].lower():

                    nameHolder = index
                    nameCounter += 1

                    if nameCounter > 1:
                        return None
        return nameHolder
#----------------------------------------#
#                Test Env                #
#----------------------------------------#
tester = SinaiMerger('mount_sinai_provider_data.json','output.json')
tester.mergeSets()
