package com.morpheusdata.netbox

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.NetworkUtility
import com.morpheusdata.core.IPAMProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.util.ConnectionUtils
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.AccountIntegration
import com.morpheusdata.model.Icon
import com.morpheusdata.model.NetworkDomain
import com.morpheusdata.model.NetworkPool
import com.morpheusdata.model.NetworkPoolIp
import com.morpheusdata.model.NetworkPoolRange
import com.morpheusdata.model.NetworkPoolServer
import com.morpheusdata.model.NetworkPoolType
import com.morpheusdata.model.OptionType
import com.morpheusdata.model.projection.NetworkPoolIdentityProjection
import com.morpheusdata.model.projection.NetworkPoolIpIdentityProjection
import com.morpheusdata.response.ServiceResponse
import groovy.json.JsonSlurper
import groovy.text.SimpleTemplateEngine
import groovy.json.JsonOutput
import groovy.util.logging.Slf4j
import io.reactivex.Completable
import io.reactivex.Single
import org.apache.commons.net.util.SubnetUtils
import io.reactivex.schedulers.Schedulers
import org.apache.http.entity.ContentType
import io.reactivex.Observable
import org.apache.commons.validator.routines.InetAddressValidator
import java.net.InetAddress
import java.util.Calendar

@Slf4j
class NetBoxProvider implements IPAMProvider {
	MorpheusContext morpheusContext
	Plugin plugin
    static String rangesPath = 'api/ipam/ip-ranges/'
    static String prefixesPath = 'api/ipam/prefixes/'
    static String getIpsPath = 'api/ipam/ip-addresses/'
    static String authPath = 'api/users/tokens/provision/'
    static String tagsPath = 'api/extras/tags/'

	static String LOCK_NAME = 'netbox.ipam'
	private java.lang.Object maxResults

	NetBoxProvider(Plugin plugin, MorpheusContext morpheusContext) {
		this.morpheusContext = morpheusContext
		this.plugin = plugin
	}

	/**
	 * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
	 *
	 * @return an implementation of the MorpheusContext for running Future based rxJava queries
	 */
	@Override
	MorpheusContext getMorpheus() {
		return morpheusContext
	}

	/**
	 * Returns the instance of the Plugin class that this provider is loaded from
	 * @return Plugin class contains references to other providers
	 */
	@Override
	Plugin getPlugin() {
		return plugin
	}

	/**
	 * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
	 * that is seeded or generated related to this provider will reference it by this code.
	 * @return short code string that should be unique across all other plugin implementations.
	 */
	@Override
	String getCode() {
		return 'netbox'
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return 'NetBox'
	}

	/**
	 * Validation Method used to validate all inputs applied to the integration of an IPAM Provider upon save.
	 * If an input fails validation or authentication information cannot be verified, Error messages should be returned
	 * via a {@link ServiceResponse} object where the key on the error is the field name and the value is the error message.
	 * If the error is a generic authentication error or unknown error, a standard message can also be sent back in the response.
	 *
	 * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
	 * @return A response is returned depending on if the inputs are valid or not.
	 */
	@Override
	ServiceResponse verifyNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
		ServiceResponse<NetworkPoolServer> rtn = ServiceResponse.error()
		rtn.errors = [:]

        if(!poolServer.name || poolServer.name == ''){
            rtn.errors['name'] = 'Name is required'
        }
		if(!poolServer.serviceUrl || poolServer.serviceUrl == ''){
			rtn.errors['serviceUrl'] = 'NetBox API URL is required'
		}
        if(poolServer.credentialData.type == 'api-key') {
            if((!poolServer.credentialData?.password || poolServer.credentialData?.password == '')){
                rtn.errors['servicePassword'] = 'Password is required'
            }
        } else if(poolServer.credentialData.type == 'username-password') {
            if((!poolServer.serviceUsername || poolServer.serviceUsername == '') && (!poolServer.credentialData?.username || poolServer.credentialData?.username == '')){
                rtn.errors['serviceUsername'] = 'Username is required'
            }
            if((!poolServer.servicePassword || poolServer.servicePassword == '') && (!poolServer.credentialData?.password || poolServer.credentialData?.password == '')){
                rtn.errors['servicePassword'] = 'Password is required'
            }
        }
		

		rtn.data = poolServer
		if(rtn.errors.size() > 0){
			rtn.success = false
			return rtn //
		}
        def rpcConfig = getRpcConfig(poolServer)
		HttpApiClient netboxClient = new HttpApiClient()
        def tokenResults
		try {
			def apiUrl = cleanServiceUrl(poolServer.serviceUrl)
			boolean hostOnline = false
			try {
				def apiUrlObj = new URL(apiUrl)
				def apiHost = apiUrlObj.host
				def apiPort = apiUrlObj.port > 0 ? apiUrlObj.port : (apiUrlObj?.protocol?.toLowerCase() == 'https' ? 443 : 80)
				hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, true, true, null)
			} catch(e) {
				log.error("Error parsing URL {}", apiUrl, e)
			}
			if(hostOnline) {
				opts.doPaging = false
				opts.maxResults = 1
                tokenResults = login(netboxClient,rpcConfig)
                if(tokenResults.success) {
				    def networkList = listNetworks(netboxClient,tokenResults.token.toString(),poolServer, opts)
                    if(networkList.success) {
                        rtn.success = true
                    } else {
                        rtn.msg = networkList.msg ?: 'Error connecting to NetBox'
                    }
                } else {
                    rtn.msg = tokenResults.msg ?: 'Error authenticating to NetBox'
                }
            } else {
                rtn.msg = 'Host not reachable'
            }
		} catch(e) {
			log.error("verifyPoolServer error: ${e}", e)
		} finally {
			netboxClient.shutdownClient()
		}
		return rtn
	}

	ServiceResponse<NetworkPoolServer> initializeNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        log.info("initializeNetworkPoolServer {} with id {}", poolServer.name, poolServer.id)
		log.debug("initializeNetworkPoolServer: ${poolServer.dump()}")
		def rtn = new ServiceResponse()
        try {
            if(poolServer) {
                refresh(poolServer)
                rtn.data = poolServer
            } else {
                rtn.error = 'No pool server found'
            }
        } catch(e) {
            rtn.error = "initializeNetworkPoolServer error: ${e}"
            log.error("initializeNetworkPoolServer error: ${e}", e)
        }
        return rtn
    }

	@Override
	ServiceResponse createNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
		log.info "createNetworkPoolServer() no-op"
		return ServiceResponse.success() // no-op
	}

	@Override
	ServiceResponse updateNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
		return ServiceResponse.success() // no-op
	}

	protected ServiceResponse refreshNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
		def rtn = new ServiceResponse()
        def tokenResults
        def rpcConfig = getRpcConfig(poolServer)
		log.debug("refreshNetworkPoolServer: {}", poolServer.dump())
		HttpApiClient netboxClient = new HttpApiClient()
		netboxClient.throttleRate = poolServer.serviceThrottleRate
		try {
			def apiUrl = cleanServiceUrl(poolServer.serviceUrl)
			def apiUrlObj = new URL(apiUrl)
			def apiHost = apiUrlObj.host
			def apiPort = apiUrlObj.port > 0 ? apiUrlObj.port : (apiUrlObj?.protocol?.toLowerCase() == 'https' ? 443 : 80)
			def hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, true, true, null)

			log.debug("online: {} - {}", apiHost, hostOnline)

			def testResults
			// Promise
			if(hostOnline) {
                tokenResults = login(netboxClient,rpcConfig)
                if(tokenResults.success) {
				    testResults = testNetworkPoolServer(netboxClient,tokenResults.token as String,poolServer) as ServiceResponse<Map>
                    if(!testResults.success) {
                        //NOTE invalidLogin was only ever set to false.
                        morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'Error calling NetBox').blockingGet()
                    } else {
                        morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.syncing).blockingGet()
                    }
                } else {
                    morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'NetBox api not reachable')
                    return ServiceResponse.error("NetBox api not reachable")
                }
            } else {
                morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'NetBox api not reachable')
            }
			Date now = new Date()
			if(testResults?.success) {
                String token = tokenResults?.token as String
				cacheNetworks(netboxClient,token,poolServer)
				if(poolServer?.configMap?.inventoryExisting) {
					cacheIpAddressRecords(netboxClient,token,poolServer)
				}
				log.info("Sync Completed in ${new Date().time - now.time}ms")
				morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.ok).subscribe().dispose()
			}
			return testResults
		} catch(e) {
			log.error("refreshNetworkPoolServer error: ${e}", e)
		} finally {
            if(tokenResults?.success) {
                logout(netboxClient,rpcConfig,tokenResults.token as String)
            }
			netboxClient.shutdownClient()
		}
		return rtn
	}

	// cacheNetworks methods
	void cacheNetworks(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts = [:]) {
		opts.doPaging = true
		def listResults = listNetworks(client, token, poolServer)
		if(listResults.success && listResults.data) {
			List apiItems = listResults.data as List<Map>
			Observable<NetworkPoolIdentityProjection> poolRecords = morpheus.network.pool.listIdentityProjections(poolServer.id)
			SyncTask<NetworkPoolIdentityProjection,Map,NetworkPool> syncTask = new SyncTask(poolRecords, apiItems as Collection<Map>)
			syncTask.addMatchFunction { NetworkPoolIdentityProjection domainObject, Map apiItem ->
				domainObject.externalId == "${apiItem.id}" && ((apiItem.prefix && ["netboxprefix","netboxprefixipv6"].contains(domainObject.typeCode)) || (!apiItem.prefix && ["netbox","netboxipv6"].contains(domainObject.typeCode)) )
			}.onDelete {removeItems ->
				morpheus.network.pool.remove(poolServer.id, removeItems).blockingGet()
			}.onAdd { itemsToAdd ->
				addMissingPools(poolServer, itemsToAdd)
			}.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkPoolIdentityProjection,Map>> updateItems ->

				Map<Long, SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
				return morpheus.network.pool.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkPool pool ->
					SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map> matchItem = updateItemMap[pool.id]
					return new SyncTask.UpdateItem<NetworkPool,Map>(existingItem:pool, masterItem:matchItem.masterItem)
				}

			}.onUpdate { List<SyncTask.UpdateItem<NetworkPool,Map>> updateItems ->
				updateMatchedPools(poolServer, updateItems)
			}.start()
		}
	}

	void addMissingPools(NetworkPoolServer poolServer, Collection<Map> chunkedAddList) {
        HttpApiClient client = new HttpApiClient();
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
		List<NetworkPool> missingPoolsList = []
		chunkedAddList?.each { Map it ->
			def networkIp = it.display
			def newNetworkPool
            def cidr = it?.start_address ?: it?.prefix
            def startAddress = it?.start_address ? it?.start_address?.tokenize('/')[0] : it?.prefix?.tokenize('/')[0]
            def endAddress = it?.end_address ? it?.end_address?.tokenize('/')[0] : it?.prefix?.tokenize('/')[0]
            def size = it?.size ?: 0
			def rangeConfig
            def addRange
            def poolType

            log.debug("CIDR: ${cidr} and startAddress: ${startAddress}")

			if(it.family.value == 4) {
                if(it.prefix) {
                    poolType = new NetworkPoolType(code: 'netboxprefix')
                    def networkInfo = getNetworkPoolConfig(it.prefix)
                    def addConfig = [account:poolServer.account, poolServer:poolServer, owner:poolServer.account, name:it.display, externalId:"${it.id}",
                                    cidr: cidr,type: poolType, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id,ipCount:networkInfo.config.ipCount]
                    newNetworkPool = new NetworkPool(addConfig)
                    newNetworkPool.ipRanges = []
                    networkInfo.ranges?.each { range ->
                        log.debug("range: ${range}")
                        rangeConfig = [networkPool:newNetworkPool, startAddress:range.startAddress, endAddress:range.endAddress, addressCount:networkInfo.config.ipCount]
                        addRange = new NetworkPoolRange(rangeConfig)
                        newNetworkPool.ipRanges.add(addRange)
                    }
                } else {
                    poolType = new NetworkPoolType(code: 'netbox')
                    def addConfig = [account:poolServer.account, poolServer:poolServer, owner:poolServer.account, name:it.display, externalId:"${it.id}",
                                    cidr: cidr, type: poolType, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id,ipCount: size]
                    newNetworkPool = new NetworkPool(addConfig)
                    newNetworkPool.ipRanges = []
                    rangeConfig = [cidr:cidr, startAddress:startAddress, endAddress:endAddress, addressCount:size]
                    addRange = new NetworkPoolRange(rangeConfig)
                    newNetworkPool.ipRanges.add(addRange)
                }
            }
            if(it.family.value == 6) {
                if(it.prefix) {
                    poolType = new NetworkPoolType(code: 'netboxprefixipv6')
                    if (startAddress.endsWith(':')) {
                        startAddress = startAddress + '0'
                        endAddress = endAddress + '0'
                    }
                } else {
                    poolType = new NetworkPoolType(code: 'netboxipv6')
                }
                
                def addConfig = [account:poolServer.account, poolServer:poolServer, owner:poolServer.account, name:it.display, externalId:"${it.id}",
                                cidr: cidr, type: poolType, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id,ipCount: size]
                newNetworkPool = new NetworkPool(addConfig)
                newNetworkPool.ipRanges = []
                rangeConfig = [cidrIPv6: cidr, startIPv6Address: startAddress, endIPv6Address: endAddress,addressCount:size]
                addRange = new NetworkPoolRange(rangeConfig)
                newNetworkPool.ipRanges.add(addRange)
			}
			missingPoolsList.add(newNetworkPool)
		}
		morpheus.network.pool.create(poolServer.id, missingPoolsList).blockingGet()
	}

	void updateMatchedPools(NetworkPoolServer poolServer, List<SyncTask.UpdateItem<NetworkPool,Map>> chunkedUpdateList) {
        HttpApiClient client = new HttpApiClient();
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
		List<NetworkPool> poolsToUpdate = []
		chunkedUpdateList?.each { update ->
			NetworkPool existingItem = update.existingItem
            Map network = update.masterItem
            def count = network?.size ?: 0

			if(existingItem) {
				//update view ?
				def save = false
				def networkIp = network?.start_address ?: network?.prefix
				def displayName = network.display
				if(existingItem?.displayName != displayName) {
					existingItem.displayName = displayName
					save = true
				}
				if(existingItem?.cidr != networkIp) {
					existingItem.cidr = networkIp
					save = true
				}
                if(existingItem?.ipCount != count && !network.prefix) {
					existingItem.ipCount = count
					save = true
				}
                if(save) {
                    poolsToUpdate << existingItem
                }
            }
        }
		if(poolsToUpdate.size() > 0) {
			morpheus.network.pool.save(poolsToUpdate).blockingGet()
		}
	}
	
	@Override
	ServiceResponse createHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp, NetworkDomain domain, Boolean createARecord, Boolean createPtrRecord) {
		HttpApiClient client = new HttpApiClient();
        InetAddressValidator inetAddressValidator = new InetAddressValidator()
        
        def rpcConfig = getRpcConfig(poolServer)
        def token
        def tokenResults
        
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)

        try {
            tokenResults = login(client,rpcConfig)
            def results = []
            if (tokenResults.success) {
                def hostname = networkPoolIp.hostname
                token = tokenResults.token.toString()
                requestOptions.headers = [Authorization: "Token ${token}".toString()]
                
                if(domain && hostname && !hostname.endsWith(domain.name))  {
                    hostname = "${hostname}.${domain.name}"
                }

                def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                def apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath
                def externalId
                def newIpPath
                def tags

                if(poolServer.configMap?.tags) {
                    tags = new JsonSlurper().parseText(addTags(poolServer.configMap?.tags))
				}
                
                if (networkPool.type.code.contains('prefix')) {
                    newIpPath = prefixesPath
                } else {
                    newIpPath = rangesPath
                }

                if(networkPoolIp.ipAddress) {
                    // Make sure it's a valid IP
                    if (inetAddressValidator.isValidInet4Address(networkPoolIp.ipAddress)) {
                        log.info("A Valid IPv4 Address Entered: ${networkPoolIp.ipAddress}")
                    } else if (inetAddressValidator.isValidInet6Address(networkPoolIp.ipAddress)) {
                        log.info("A Valid IPv6 Address Entered: ${networkPoolIp.ipAddress}")
                    } else {
                        log.error("Invalid IP Address Requested: ${networkPoolIp.ipAddress}", results)
                        return ServiceResponse.error("Invalid IP Address Requested: ${networkPoolIp.ipAddress}")
                    }

                    requestOptions.queryParams = ['address':networkPoolIp.ipAddress + '/' + networkPool.cidr.tokenize('/')[1]]
                    // Check IP Usage
                    results = client.callJsonApi(apiUrl,apiPath,requestOptions,'GET')

                    if (results?.success && !results?.error) {
                        if (!results?.data.results) {
                            // If Empty, Create the IP
                            apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath
                            requestOptions.queryParams = [:]
                            requestOptions.body = JsonOutput.toJson(['address':networkPoolIp.ipAddress + '/' + networkPool.cidr.tokenize('/')[1],'status':'reserved',"dns_name":hostname,'tags':tags ?: []])

                            results = client.callJsonApi(apiUrl,apiPath,requestOptions,'POST')

                        } else if (results?.data?.results){
                            // If Reserved
                            externalId = results.data.results.id
                            apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath + externalId + '/'
                            requestOptions.queryParams = [:]
                            requestOptions.body = JsonOutput.toJson(['address':networkPoolIp.ipAddress + '/' + networkPool.cidr.tokenize('/')[1],'status':'reserved',"dns_name":hostname,'tags':tags ?: []])

                            results = client.callJsonApi(apiUrl,apiPath,requestOptions,'PUT')
                        } else {
                            log.error("Allocate IP Error: ${e}", e)
                        }
                    } else {
                        log.error("Allocate IP Error: ${e}", e)
                    }
                } else {
                    // Grab next available IP
                    apiPath = getServicePath(rpcConfig.serviceUrl) + newIpPath
                    requestOptions.queryParams = [:]
                    requestOptions.body = null
                    results = client.callJsonApi(apiUrl,apiPath + networkPool.externalId + '/available-ips/',requestOptions,'POST')

                    if(results.success && !results.error) {
                        externalId = results.data.id
                        requestOptions.body = JsonOutput.toJson(['address':results.data.address,'status':'reserved',"dns_name":hostname,'tags':tags ?: []])
                        apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath + externalId + '/'
                        
                        results = client.callJsonApi(apiUrl,apiPath,requestOptions,'PUT')
                    }
                }

                if (results.success && !results.error) {
                    networkPoolIp.externalId = results.data.id
                    networkPoolIp.ipAddress = results.data.address.tokenize('/')[0]
                    networkPoolIp = morpheus.network.pool.poolIp.create(networkPoolIp)?.blockingGet()
                    return ServiceResponse.success(networkPoolIp)
                } else {
                    log.warn("API Call Failed to allocate IP Address")
                    return ServiceResponse.error("API Call Failed to allocate IP Address",null,networkPoolIp)
                }
            }
        } catch(e) {
            log.warn("API Call Failed to allocate IP Address {}",e)
            return ServiceResponse.error("API Call Failed to allocate IP Address",null,networkPoolIp)
        } finally {
            if(tokenResults?.success) {
                logout(client,rpcConfig,token)
            }
            client.shutdownClient()
        }
	}

	@Override
	ServiceResponse updateHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp) {
		HttpApiClient client = new HttpApiClient();
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
        def token

        try {
            def tokenResults = login(client,rpcConfig)
            def results = []
            def hostname = networkPoolIp.hostname

            if (tokenResults?.success) {
                token = tokenResults?.token.toString()
                requestOptions.headers = [Authorization: "Token ${token}".toString()]
                def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                def apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath
                def externalId = networkPoolIp.externalId.toString() + '/'

                requestOptions.body = JsonOutput.toJson(['address':networkPoolIp.ipAddress + '/' + networkPool.cidr.tokenize('/')[1],"dns_name":hostname])

                results = client.callJsonApi(apiUrl,apiPath + externalId,null,null,requestOptions,'PUT')

                if (results?.success) {
                    return ServiceResponse.success(networkPoolIp)
                } else {
				    return ServiceResponse.error(results.error ?: 'Error Updating Host Record', null, networkPoolIp)
			}
            } else {
                return ServiceResponse.error("Error Authenticating with NetBox",null,networkPoolIp)
            }
        } catch(ex) {
            log.error("Error Updating Host Record {}",ex.message,ex)
            return ServiceResponse.error("Error Updating Host Record ${ex.message}",null,networkPoolIp)
        } finally {
            if(token) {
                logout(client,rpcConfig,token)
            }
            client.shutdownClient()
        }
	}

	@Override
	ServiceResponse deleteHostRecord(NetworkPool networkPool, NetworkPoolIp poolIp, Boolean deleteAssociatedRecords ) {
		HttpApiClient client = new HttpApiClient();
        def poolServer = morpheus.network.getPoolServerById(networkPool.poolServer.id).blockingGet()
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
        def token

        try {
            def tokenResults = login(client,rpcConfig)
            def results = []
            if (tokenResults?.success) {
                token = tokenResults.token.toString()
                requestOptions.headers = [Authorization: "Token ${token}".toString()]
                def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                def apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath
                def externalId = poolIp.externalId.toString() + '/'

                if(poolServer?.configMap?.deprecate){
                    requestOptions.body = JsonOutput.toJson(['address':poolIp.ipAddress + '/' + networkPool.cidr.tokenize('/')[1],"status":"deprecated"])

                    results = client.callJsonApi(apiUrl,apiPath + externalId,null,null,requestOptions,'PUT')

                    if (results?.success) {
                        return ServiceResponse.success(poolIp)
                    } else {
                        return ServiceResponse.error(results.error ?: 'Error Updating Host Record', null, poolIp)
                    }
                } else {
                    results = client.callJsonApi(apiUrl,apiPath + externalId,null,null,requestOptions,'DELETE')

                    if(!results?.success && results?.data?.detail == 'Not found.') {
                        return ServiceResponse.success(poolIp)
                    } else if (results?.success && !results?.error) {
                        return ServiceResponse.success(poolIp)
                    } else {
                        log.error("Error Deleting Host Record ${poolIp}")
                        return ServiceResponse.error("Error Deleting Host Record ${poolIp}")
                    }
                }
            } else {
                log.error("Error Authenticating with NetBox")
                return ServiceResponse.error("Error Authenticating with NetBox",null,poolIp)
            }
        } catch(x) {
            log.error("Error Deleting Host Record {}",x.message,x)
            return ServiceResponse.error("Error Deleting Host Record ${x.message}",null,poolIp)
        } finally {
            if(token) {
                logout(client,rpcConfig,token)
            }
            client.shutdownClient()
        }
	}

	private ServiceResponse listNetworks(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts = [:]) {
        def rtn = new ServiceResponse()
        rtn.data = [] // Initialize rtn.data as an empty list
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def endpoints = [rangesPath,prefixesPath]
            HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
            requestOptions.headers = [Authorization: "Token ${token}".toString()]

            log.debug("url: ${apiUrl} path: ${apiPath}")

            endpoints.each{ String ep ->
                def hasMore = true
                def attempt = 0
                def start = 0
                def doPaging = opts.doPaging != null ? opts.doPaging : true
                def maxResults = opts.maxResults ?: 1000
                def apiPath = getServicePath(rpcConfig.serviceUrl) + ep

                if(doPaging == true) {    
                    while(hasMore && attempt < 1000) {
                        attempt++

                        requestOptions.queryParams = [limit:maxResults.toString(),offset:start.toString(),'status__n':ep == rangesPath ? 'deprecated' : 'container']
                        def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                        if(results?.success && results?.error != true) {

                            rtn.success = true
                            if(results.data?.results?.size() > 0) {
                                
                                rtn.data += results.data.results

                                if(!results.data.next) {
                                    hasMore = false
                                } else {
                                    start += maxResults
                                }
                
                            } else {
                                hasMore = false
                            }
                        } else {
                            hasMore = false

                            if(!rtn.success) {
                                rtn.msg = results.error
                            }
                        }
                    }
                } else {
                    requestOptions.queryParams = [limit:maxResults.toString(),offset:start.toString(),'status__n':ep == rangesPath ? 'deprecated' : 'container']
                    def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                    if(results?.success && results?.error != true) {
                        rtn.success = true
                        if(results.data?.result?.size() > 0) {
                            rtn.data = results.data.results
                        }
                    } else {
                        if(!rtn.success) {
                            rtn.msg = results.error
                        }
                    }
                }
            }
        } catch(e) {
            log.error("listNetworks error: ${e}", e)
        }
        log.debug("List Networks Results: ${rtn}")
        return rtn
	}

	// cacheIpAddressRecords
    void cacheIpAddressRecords(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts=[:]) {
        morpheus.network.pool.listIdentityProjections(poolServer.id).buffer(50).concatMap { Collection<NetworkPoolIdentityProjection> poolIdents ->
            return morpheus.network.pool.listById(poolIdents.collect{it.id})
        }.concatMap { NetworkPool pool ->
            def listResults = listHostRecords(client,token,poolServer,pool)
            if (listResults.success && listResults.data) {

                List<Map> apiItems = listResults.data
                Observable<NetworkPoolIpIdentityProjection> poolIps = morpheus.network.pool.poolIp.listIdentityProjections(pool.id)
                SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp> syncTask = new SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp>(poolIps, apiItems)
                return syncTask.addMatchFunction { NetworkPoolIpIdentityProjection domainObject, Map apiItem ->
                    domainObject.externalId == "${apiItem.id}"
                }.addMatchFunction { NetworkPoolIpIdentityProjection domainObject, Map apiItem ->
                    domainObject.ipAddress == apiItem.address.tokenize('/')[0]
                }.onDelete {removeItems ->
                    morpheus.network.pool.poolIp.remove(pool.id, removeItems).blockingGet()
                }.onAdd { itemsToAdd ->
                    addMissingIps(pool, itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection,Map>> updateItems ->

                    Map<Long, SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheus.network.pool.poolIp.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkPoolIp poolIp ->
                        SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection, Map> matchItem = updateItemMap[poolIp.id]
                        return new SyncTask.UpdateItem<NetworkPoolIp,Map>(existingItem:poolIp, masterItem:matchItem.masterItem)
                    }

                }.onUpdate { List<SyncTask.UpdateItem<NetworkDomain,Map>> updateItems ->
                    updateMatchedIps(updateItems)
                }.observe()
            } else {
                return Single.just(false).toObservable()
            }
        }.doOnError{ e ->
            log.error("cacheIpRecords error: ${e}", e)
        }.blockingSubscribe()

    }

	void addMissingIps(NetworkPool pool, List addList) {
        List<NetworkPoolIp> poolIpsToAdd = addList?.collect { it ->
			def ipAddress = it.address.tokenize('/')[0]
			def types = it.status.value
			def ipType = 'assigned'
			if(types == 'reserved') {
				ipType = 'reserved'
			} else if (types == 'deprecated') {
                ipType = 'unmanaged'
            }
			def addConfig = [networkPool: pool, networkPoolRange: pool.ipRanges ? pool.ipRanges.first() : null, ipType: ipType, hostname: it.dns_name, ipAddress: ipAddress, externalId:it.id]
			def newObj = new NetworkPoolIp(addConfig)
			return newObj

		}
		if(poolIpsToAdd.size() > 0) {
			morpheus.network.pool.poolIp.create(pool, poolIpsToAdd).blockingGet()
		}
	}

	void updateMatchedIps(List<SyncTask.UpdateItem<NetworkPoolIp,Map>> updateList) {
		List<NetworkPoolIp> ipsToUpdate = []
		updateList?.each {  update ->
			NetworkPoolIp existingItem = update.existingItem

			if(existingItem) {
				def hostname = update.masterItem.dns_name
                def types = update.masterItem.status.value
				def ipType = 'assigned'
                if(types == 'reserved') {
                    ipType = 'reserved'
                } else if (types == 'deprecated') {
                    ipType = 'unmanaged'
                }
				def save = false
				if(existingItem.ipType != ipType) {
					existingItem.ipType = ipType
					save = true
				}
				if(existingItem.hostname != hostname) {
					existingItem.hostname = hostname
					save = true
				}
				if(save) {
					ipsToUpdate << existingItem
				}
			}
		}
		if(ipsToUpdate.size() > 0) {
			morpheus.network.pool.poolIp.save(ipsToUpdate).blockingGet()
		}
	}


	private ServiceResponse listHostRecords(HttpApiClient client, String token, NetworkPoolServer poolServer,NetworkPool networkPool, Map opts = [:]) {
        def rtn = new ServiceResponse()
        rtn.data = [] // Initialize rtn.data as an empty list
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
            requestOptions.headers = [Authorization: "Token ${token}".toString()]

            def startAddress = networkPool?.ipRanges?[0].startAddress ?: null
            def endAddress = networkPool?.ipRanges?[0].endAddress ?: null
            def hasMore = true
            def attempt = 0
            def start = 0
            def doPaging = opts.doPaging != null ? opts.doPaging : true
            def maxResults = opts.maxResults ?: 1000
            def apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath

            log.debug("url: ${apiUrl} path: ${apiPath}")

            if(doPaging == true) {
                while(hasMore && attempt < 1000) {
                    attempt++

                    requestOptions.queryParams = [limit:maxResults.toString(),offset:start.toString(),'parent':networkPool.cidr.toString()]
                    def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                    if(results?.success && results?.error != true) {

                        rtn.success = true
                        if(results.data?.results?.size() > 0) {
                            results.data.results.each { it ->
                                if (networkPool.cidr.tokenize("/")[1] == it.address.tokenize('/')[1]) {
                                    if (!startAddress || !endAddress) {
                                        rtn.data += it
                                    } else if (isIPInRange(it.address.tokenize('/')[0],startAddress,endAddress)) {
                                        rtn.data += it
                                    }
                                }
                            }

                            if(!results.data.next) {
                                hasMore = false
                            } else {
                                start += maxResults
                            }
            
                        } else {
                            hasMore = false
                        }
                    } else {
                        hasMore = false

                        if(!rtn.success) {
                            rtn.msg = results.error
                        }
                    }
                }
            } else {
                requestOptions.queryParams = [limit:maxResults.toString(),offset:start.toString(),'parent':networkPool.cidr.toString()]
                def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                    if(results.data?.result?.size() > 0) {
                        rtn.data = results.data.results
                    }
                } else {
                    if(!rtn.success) {
                        rtn.msg = results.error
                    }
                }
            }
        } catch(e) {
            log.error("listNetworks error: ${e}", e)
        }
        log.debug("List Networks Results: ${rtn}")
        return rtn
	}

	ServiceResponse testNetworkPoolServer(HttpApiClient client, String token, NetworkPoolServer poolServer) {
		def rtn = new ServiceResponse()
		try {
			def opts = [doPaging:false, maxResults:1]
			def networkList = listNetworks(client, token, poolServer, opts)
			rtn.success = networkList.success
			rtn.data = [:]
			if(!networkList.success) {
				rtn.msg = 'error connecting to NetBox'
			}
		} catch(e) {
			rtn.success = false
			log.error("test network pool server error: ${e}", e)
		}
		return rtn
	}

    /**
     * Periodically called to refresh and sync data coming from the relevant integration. Most integration providers
     * provide a method like this that is called periodically (typically 5 - 10 minutes). DNS Sync operates on a 10min
     * cycle by default. Useful for caching Host Records created outside of Morpheus.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     */
    @Override
    void refresh(NetworkPoolServer poolServer) {
        refreshNetworkPoolServer(poolServer, [:])
    }

	/**
	 * An IPAM Provider can register pool types for display and capability information when syncing IPAM Pools
	 * @return a List of {@link NetworkPoolType} to be loaded into the Morpheus database.
	 */
	Collection<NetworkPoolType> getNetworkPoolTypes() {
		return [
			new NetworkPoolType(code:'netbox', name:'NetBox', creatable:false, description:'NetBox', rangeSupportsCidr: false),
			new NetworkPoolType(code:'netboxipv6', name:'NetBox IPv6', creatable:false, description:'NetBox IPv6', rangeSupportsCidr: true, ipv6Pool:true),
            new NetworkPoolType(code:'netboxprefix', name:'NetBox Prefix', creatable:false, description:'NetBox Prefix', rangeSupportsCidr: false),
			new NetworkPoolType(code:'netboxprefixipv6', name:'NetBox Prefix IPv6', creatable:false, description:'NetBox Prefix IPv6', rangeSupportsCidr: true, ipv6Pool:true)
		]
	}

	/**
	 * Provide custom configuration options when creating a new {@link AccountIntegration}
	 * @return a List of OptionType
	 */
	@Override
	List<OptionType> getIntegrationOptionTypes() {
		return [
				new OptionType(code: 'netbox.serviceUrl', name: 'Service URL', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUrl', fieldLabel: 'API Url', fieldContext: 'domain', placeHolder: 'https://x.x.x.x/', displayOrder: 0, required:true),
				new OptionType(code: 'netbox.credentials', name: 'Credentials', inputType: OptionType.InputType.CREDENTIAL, fieldName: 'type', fieldLabel: 'Credentials', fieldContext: 'credential', required: true, displayOrder: 1, defaultValue: 'local', optionSource: 'credentials',
                    config: '{"credentialTypes":["username-password","api-key"]}', helpText: "Username + Password <or> Token Only if using Local Credentials"),

				new OptionType(code: 'netbox.serviceUsername', name: 'Service Username', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUsername', fieldLabel: 'Username', fieldContext: 'domain', displayOrder: 2,localCredential: true, required: false),
				new OptionType(code: 'netbox.servicePassword', name: 'Service Password', inputType: OptionType.InputType.PASSWORD, fieldName: 'servicePassword', fieldLabel: 'Password', fieldContext: 'domain', displayOrder: 3,localCredential: true, required: false),
                new OptionType(code: 'netbox.apiToken', name: 'API Token', inputType: OptionType.InputType.TEXT, fieldName: 'apiToken', fieldLabel: 'API Token', fieldContext: 'config', displayOrder: 4,localCredential: true),
				new OptionType(code: 'netbox.throttleRate', name: 'Throttle Rate', inputType: OptionType.InputType.NUMBER, defaultValue: 0, fieldName: 'serviceThrottleRate', fieldLabel: 'Throttle Rate', fieldContext: 'domain', displayOrder: 5),
				new OptionType(code: 'netbox.ignoreSsl', name: 'Ignore SSL', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'ignoreSsl', fieldLabel: 'Disable SSL SNI Verification', fieldContext: 'domain', displayOrder: 6),
				new OptionType(code: 'netbox.inventoryExisting', name: 'Inventory Existing', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'inventoryExisting', fieldLabel: 'Inventory Existing', fieldContext: 'config', displayOrder: 7),
                new OptionType(code: 'netbox.deprecate', name: 'Deprecate on Delete', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'deprecate', fieldLabel: 'Deprecate on Delete', fieldContext: 'config', displayOrder: 8),
                new OptionType(code: 'netbox.tags', name: 'Tags', inputType: OptionType.InputType.TEXT, fieldName: 'tags', fieldLabel: 'Tags', fieldContext: 'config', displayOrder: 9, helpText: "value|value2")
		]
	}

	@Override
	Icon getIcon() {
		return new Icon(path:"netbox.svg", darkPath: "netbox.svg")
	}

    def login(HttpApiClient client, rpcConfig) {
        def rtn = [success:false]
        def expiration = formatDate(getCurrentTimePlus5Minutes())
        if(rpcConfig.username && rpcConfig.password) {
            try {
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.headers = ['content-type':'application/json']
                requestOptions.body = JsonOutput.toJson([username:rpcConfig.username, password:rpcConfig.password, expires:expiration, description:'Generated by Morpheus'])

                def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                def apiPath = getServicePath(rpcConfig.serviceUrl) + authPath

                def results = client.callJsonApi(apiUrl,apiPath,requestOptions,'POST')
                if(results?.success && results?.error != true) {
                    log.debug("login: ${results}")
                    rtn.token = results.data?.key?.trim()
                    rtn.success = true
                } else {
                    //error
                    log.error("getToken error: ${e}", e)
                }
            } catch(e) {
                log.error("getToken error: ${e}", e)
            }
        } else if (rpcConfig.password && !rpcConfig.username) {
            rtn.token = rpcConfig.password.toString()
            rtn.success = true
        } else {
            log.error("getToken error: ${e}", e)
        }
        return rtn
    }

    void logout(HttpApiClient client, rpcConfig, String token) {
        try {
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + 'logout'
            HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
            requestOptions.headers = [Authorization: "Token ${token}".toString()]
            client.callJsonApi(apiUrl,apiPath,requestOptions,"GET")
        } catch(e) {
            log.error("logout error: ${e}", e)
        }
    }

    private getRpcConfig(NetworkPoolServer poolServer) {
        return [
            username:poolServer.credentialData?.username ?: poolServer.serviceUsername,
            password:poolServer.credentialData?.password ?: poolServer.servicePassword ?: poolServer.configMap?.apiToken,
            serviceUrl:poolServer.serviceUrl,
            ignoreSSL:poolServer.ignoreSsl
        ]
    }

	private static String cleanServiceUrl(String url) {
		def rtn = url
		def slashIndex = rtn?.indexOf('/', 9)
		if(slashIndex > 9)
			rtn = rtn.substring(0, slashIndex)
		return rtn
	}

	private static String getServicePath(String url) {
		def rtn = '/'
		def slashIndex = url?.indexOf('/', 9)
		if(slashIndex > 9)
			rtn = url.substring(slashIndex)
		if(rtn?.endsWith('/'))
			return rtn.substring(0, rtn.lastIndexOf("/"));
		return rtn
	}

    static Map getNetworkPoolConfig(String cidr) {
        def rtn = [config:[:], ranges:[]]
        try {
            def subnetInfo = new SubnetUtils(cidr).getInfo()
            rtn.config.netmask = subnetInfo.getNetmask()
            rtn.config.ipCount = subnetInfo.getAddressCountLong() ?: 0
            rtn.config.ipFreeCount = rtn.config.ipCount
            rtn.ranges << [startAddress:subnetInfo.getLowAddress(), endAddress:subnetInfo.getHighAddress()]
        } catch(e) {
            log.warn("error parsing network pool cidr: ${e}", e)
        }
        return rtn
    }

    def isIPInRange(ipAddress, startRange, endRange) {
        try {
            def start = InetAddress.getByName(startRange)
            def end = InetAddress.getByName(endRange)
            def ip = InetAddress.getByName(ipAddress)

            log.debug("StartIP: ${start}, endAddress: ${end}, ip: ${ip}")

            // Check for IPv4 or IPv6
            if (ip instanceof Inet4Address && start instanceof Inet4Address && end instanceof Inet4Address) {
                // IPv4
                // def ipInt = (ip.getAddress()[0] << 24) | (ip.getAddress()[1] << 16) | (ip.getAddress()[2] << 8) | ip.getAddress()[3]
                // def startInt = (start.getAddress()[0] << 24) | (start.getAddress()[1] << 16) | (start.getAddress()[2] << 8) | start.getAddress()[3]
                // def endInt = (end.getAddress()[0] << 24) | (end.getAddress()[1] << 16) | (end.getAddress()[2] << 8) | end.getAddress()[3]
                def ipInt = ipv4ToInteger(ip.toString())
                def endInt = ipv4ToInteger(end.toString())
                def startInt = ipv4ToInteger(start.toString())
                log.debug("startInt: ${startInt}, endInt: ${endInt}, ipInt: ${ipInt}")
                return ipInt >= startInt && ipInt <= endInt
            } else if (ip instanceof Inet6Address && start instanceof Inet6Address && end instanceof Inet6Address) {
                // IPv6
                def ipBytes = ip.getAddress()
                def startBytes = start.getAddress()
                def endBytes = end.getAddress()
                return ipBytes >= startBytes && ipBytes <= endBytes
            } else {
                // Invalid IP version
                return false
            }
        } catch (e) {
            // Handle invalid IP address
            log.error("cacheIpAddress  error: ${e}", e)
        }
    }

    def addTags(String tags) {
        // Split the string into individual values using '|'
        def tagValues = tags.split("\\|")

        // Create a list of JSON objects
        def tagObjects = tagValues.collect { value ->
            [
                name: value
            ]
        }

        def jsonTags = JsonOutput.toJson(tagObjects)

        return jsonTags
    }

    def getCurrentTimePlus5Minutes() {
        Calendar calendar = Calendar.getInstance()
        calendar.add(Calendar.MINUTE, 5)

        return calendar.time
    }

    def formatDate(Object date, String outputFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") {
        def rtn
        try {
            if(date) {
                if(date instanceof Date)
                    rtn = date.format(outputFormat, TimeZone.getTimeZone('GMT'))
                else if(date instanceof CharSequence)
                    rtn = date
            }
        } catch(ignored) { }
        return rtn
    }

    def ipv4ToInteger(ipAddress) {
        if (ipAddress.startsWith("/")) {
            ipAddress = ipAddress.substring(1)
        }
        def parts = ipAddress.split('\\.') // Split the IP address into octets

        def result = 0
            for (int i = 0; i < 4; i++) {
                def octet = parts[i].toInteger()
                result = (result << 8) | octet
            }

        return result
    }
}
