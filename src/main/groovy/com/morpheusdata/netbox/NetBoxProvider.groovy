package com.morpheusdata.netbox

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.NetworkUtility
import com.morpheusdata.core.DNSProvider
import com.morpheusdata.core.IPAMProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.util.ConnectionUtils
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.AccountIntegration
import com.morpheusdata.model.Icon
import com.morpheusdata.model.NetworkDomain
import com.morpheusdata.model.NetworkDomainRecord
import com.morpheusdata.model.NetworkPool
import com.morpheusdata.model.NetworkPoolIp
import com.morpheusdata.model.NetworkPoolRange
import com.morpheusdata.model.NetworkPoolServer
import com.morpheusdata.model.NetworkPoolType
import com.morpheusdata.model.OptionType
import com.morpheusdata.model.projection.NetworkDomainIdentityProjection
import com.morpheusdata.model.projection.NetworkDomainRecordIdentityProjection
import com.morpheusdata.model.projection.NetworkPoolIdentityProjection
import com.morpheusdata.model.projection.NetworkPoolIpIdentityProjection
import com.morpheusdata.response.ServiceResponse
import groovy.json.JsonSlurper
import groovy.text.SimpleTemplateEngine
import groovy.json.JsonOutput
import groovy.util.logging.Slf4j
import io.reactivex.Completable
import io.reactivex.Single
import io.reactivex.schedulers.Schedulers
import org.apache.http.entity.ContentType
import io.reactivex.Observable
import org.apache.commons.validator.routines.InetAddressValidator

@Slf4j
class NetBoxProvider implements IPAMProvider, DNSProvider {
	MorpheusContext morpheusContext
	Plugin plugin
    static String networksPath = 'api/ipam/ip-ranges'
    static String authPath = 'api/users/tokens/provision/'

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
            rtn.errors['name'] = 'name is required'
        }
		if(!poolServer.serviceUrl || poolServer.serviceUrl == ''){
			rtn.errors['serviceUrl'] = 'NetBox API URL is required'
		}
		if((!poolServer.serviceUsername || poolServer.serviceUsername == '') && (!poolServer.credentialData?.username || poolServer.credentialData?.username == '')){
			rtn.errors['serviceUsername'] = 'username is required'
		}
		if((!poolServer.servicePassword || poolServer.servicePassword == '') && (!poolServer.credentialData?.password || poolServer.credentialData?.password == '')){
			rtn.errors['servicePassword'] = 'password is required'
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
				    def networkList = listNetworks(netboxClient,poolServer, opts)
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
		log.info("initializeNetworkPoolServer: ${poolServer.dump()}")
		def rtn = new ServiceResponse()
		try {
			if(poolServer) {
				rtn = refreshNetworkPoolServer(poolServer, opts)
				rtn.data = poolServer
			} else {
				rtn.error = 'No pool server found'
			}
		} catch(e) {
			rtn.error = "initializeNetworkPoolServer error: ${e}"
			log.error("initializeNetworkPoolServer error: ${e}", e)
		}
		log.info(rtn.dump())
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

	// TODO: Add to interface
	def validateService(AccountIntegration integration) { return null }
	def refreshDnsIntegration(AccountIntegration integration) { return null }

	@Override
	ServiceResponse<NetworkDomainRecord> createRecord(AccountIntegration integration, NetworkDomainRecord record, Map opts) {
		ServiceResponse<NetworkDomainRecord> rtn = new ServiceResponse<>()
		HttpApiClient client = new HttpApiClient()
		def poolServer = morpheus.network.getPoolServerByAccountIntegration(integration).blockingGet()

		try {
			if(integration) {

				def fqdn = record.name
				if(!record.name.endsWith(record.networkDomain.name)) {
					fqdn = "${record.name}.${record.networkDomain.name}"
				}


				def serviceUrl = cleanServiceUrl(poolServer.serviceUrl)
				def recordType = record.type

				def apiPath
				def results = new ServiceResponse()
				def body
				def extraAttributes
				if(poolServer.configMap?.extraAttributes) {
					extraAttributes = generateExtraAttributes(poolServer,[username: record.createdBy?.username, userId: record.createdBy?.id, dateCreated: MorpheusUtils.formatDate(new Date()) ])
				}

				switch(recordType) {
					case 'A':
						apiPath = getServicePath(poolServer.serviceUrl) + 'record:a'
						body = [
							name:fqdn,
							ipv4addr: record.content
						]
						if(extraAttributes) {
							body.extattrs = extraAttributes
						}

						results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl,body:body), 'POST')

						break
					case 'AAAA':
						apiPath = getServicePath(poolServer.serviceUrl) + 'record:aaaa'
						body = [
							name:fqdn,
							ipv6addr: record.content
						]
						if(extraAttributes) {
							body.extattrs = extraAttributes
						}

						results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl,body:body), 'POST')

						break
					case 'CNAME':
						apiPath = getServicePath(poolServer.serviceUrl) + 'record:cname'
						body = [
							name:fqdn,
							canonical: record.content
						]
						if(extraAttributes) {
							body.extattrs = extraAttributes
						}

						results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl,body:body), 'POST')

						break
					case 'TXT':
						apiPath = getServicePath(poolServer.serviceUrl) + 'record:txt'
						body = [
							name:fqdn,
							text: record.content
						]
						if(extraAttributes) {
							body.extattrs = extraAttributes
						}

						results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl,body:body), 'POST')
						break
					case 'MX':
						apiPath = getServicePath(poolServer.serviceUrl) + 'record:mx'
						body = [
							name:fqdn,
							mail_exchanger: record.content
						]
						if(extraAttributes) {
							body.extattrs = extraAttributes
						}

						results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl,body:body), 'POST')
						break
				}

				log.info("createRecord results: ${results}")
				if(results.success) {
					record.externalId = results.content.substring(1, results.content.length() - 1)
					return new ServiceResponse<NetworkDomainRecord>(true,null,null,record)
					rtn.data = record
					rtn.success = true
				}
			} else {
				log.warn("no integration")
			}
		} catch(e) {
			log.error("provisionServer error: ${e}", e)
		} finally {
			client.callApi(poolServer.serviceUrl, getServicePath(poolServer.serviceUrl) + 'logout', poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions([headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl]), 'POST')
			client.shutdownClient()
		}
		return rtn
	}

	protected ServiceResponse refreshNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
		def rtn = new ServiceResponse()
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
				    testResults = testNetworkPoolServer(netboxClient,poolServer) as ServiceResponse<Map>
                    if(!testResults.success) {
                        //NOTE invalidLogin was only ever set to false.
                        morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'error calling NetBox').blockingGet()
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
			if(testResults.success) {
                String token = tokenResults?.token as String
				cacheNetworks(netboxClient,poolServer, opts)
				if(poolServer?.configMap?.inventoryExisting) {
					cacheIpAddressRecords(netboxClient,poolServer, opts)
				}
				log.info("Sync Completed in ${new Date().time - now.time}ms")
				morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.ok).subscribe().dispose()
			}
			return testResults
		} catch(e) {
			log.error("refreshNetworkPoolServer error: ${e}", e)
		} finally {
            if(tokenResults?.success) {
                logout(bluecatClient,rpcConfig,tokenResults.token)
            }
			netboxClient.shutdownClient()
		}
		return rtn
	}

	// cacheNetworks methods
	void cacheNetworks(HttpApiClient client, NetworkPoolServer poolServer, Map opts) {
		opts.doPaging = true
		def listResults = listNetworks(client, poolServer, opts)
		log.info("listResults: {}", listResults.dump())

		if(listResults.success) {
			List apiItems = listResults.data as List<Map>
			Observable<NetworkPoolIdentityProjection> poolRecords = morpheus.network.pool.listIdentityProjections(poolServer.id)

			SyncTask<NetworkPoolIdentityProjection,Map,NetworkPool> syncTask = new SyncTask(poolRecords, apiItems as Collection<Map>)
			syncTask.addMatchFunction { NetworkPoolIdentityProjection domainObject, Map apiItem ->
				domainObject.externalId == apiItem.'_ref'
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
		def poolType = new NetworkPoolType(code: 'infoblox')
		def poolTypeIpv6 = new NetworkPoolType(code: 'infobloxipv6')
		List<NetworkPool> missingPoolsList = []
		List<NetworkPoolRange> ranges = []
		chunkedAddList?.each { Map add ->
			def networkIp = add.network
			def newNetworkPool
			def rangeConfig
            def addRange
			def networkView = add.network_view
			def displayName = networkView ? (networkView + ' ' + networkIp) : networkIp
			def networkInfo 
			if(!networkIp.contains(':')) {
				networkInfo = MorpheusUtils.getNetworkPoolConfig(networkIp)
				def addConfig = ["poolServer": poolServer, cidr: networkIp, account: poolServer.account,
								owner: poolServer.account, name:networkIp, externalId: add.'_ref', displayName: displayName,
								type: poolType, poolEnabled: true, parentType: 'NetworkPoolServer', parentId: poolServer.id]
				addConfig += networkInfo.config
				newNetworkPool =new NetworkPool(addConfig)
				newNetworkPool.ipRanges = []
				networkInfo?.ranges?.each { range ->
					rangeConfig = [startAddress: range.startAddress, endAddress: range.endAddress, addressCount: addConfig.ipCount]
					addRange = new NetworkPoolRange(rangeConfig)
					newNetworkPool.ipRanges.add(addRange)
				}
			} else {
				def addConfig = ["poolServer": poolServer, cidr: networkIp, account: poolServer.account,
								owner: poolServer.account, name:networkIp, externalId: add.'_ref', displayName: displayName,
								type: poolTypeIpv6, poolEnabled: true, parentType: 'NetworkPoolServer', parentId: poolServer.id]
                newNetworkPool = new NetworkPool(addConfig)
                newNetworkPool.ipRanges = []
                rangeConfig = [cidrIPv6: networkIp, startIPv6Address: networkIp.tokenize('/')[0], endIPv6Address: networkIp.tokenize('/')[0]]
                addRange = new NetworkPoolRange(rangeConfig)
                newNetworkPool.ipRanges.add(addRange)
			}
			missingPoolsList.add(newNetworkPool)
		}
		morpheus.network.pool.create(poolServer.id, missingPoolsList).blockingGet()
	}

	void updateMatchedPools(NetworkPoolServer poolServer, List<SyncTask.UpdateItem<NetworkPool,Map>> chunkedUpdateList) {
		List<NetworkPool> poolsToUpdate = []
		chunkedUpdateList?.each { update ->
			NetworkPool existingItem = update.existingItem
			if(existingItem) {
				//update view ?
				def save = false
				def networkIp = update.masterItem.network
				def networkView = update.masterItem.network_view
				def displayName = networkView ? (networkView + ' ' + networkIp) : networkIp
				if(existingItem?.displayName != displayName) {
					existingItem.displayName = displayName
					save = true
				}
				if(existingItem?.cidr != networkIp) {
					existingItem.cidr = networkIp
					save = true
				}
				if(!existingItem.ipRanges) {
					log.warn("no ip ranges found!")
					def networkInfo = MorpheusUtils.getNetworkPoolConfig(networkIp)
					networkInfo?.ranges?.each { range ->
						log.info("range: ${range}")
						def rangeConfig = [networkPool:existingItem, startAddress:range.startAddress, endAddress:range.endAddress, addressCount:networkInfo.config.ipCount]
						def addRange = new NetworkPoolRange(rangeConfig)
						existingItem.addToIpRanges(addRange)
					}
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
	ServiceResponse<NetworkPoolIp> createHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp, NetworkDomain domain = null, Boolean createARecord = false, Boolean createPtrRecord = false) {
		HttpApiClient client = new HttpApiClient();
		try {
			def serviceUrl = cleanServiceUrl(poolServer.serviceUrl)
			def apiPath = getServicePath(poolServer.serviceUrl) + 'record:host' //networkPool.externalId
			log.debug("url: ${serviceUrl} path: ${apiPath}")
			def hostname = networkPoolIp.hostname
			if (domain && hostname && !hostname.endsWith(domain.name)) {
				hostname = "${hostname}.${domain.name}"
			}
			String shortHostname = hostname
			def networkDomainName =  domain?.name ? ('.' + domain.name) : '.localdomain'
			if(networkDomainName && hostname.endsWith(networkDomainName)) {
				def suffixIndex = hostname.indexOf(networkDomainName)
				if(suffixIndex > -1) {
					shortHostname = hostname.substring(0,suffixIndex)
				}
			}
			def body
			def networkView = networkPool.externalId.tokenize('/')[3]
			if (networkPool.type.code == 'infoblox') {
				if(poolServer.serviceMode == 'dhcp' && networkPoolIp.macAddress) {
					body = [
						name: shortHostname,
						network_view: networkView,
						ipv4addrs: [[configure_for_dhcp: true, mac: networkPoolIp.macAddress, ipv4addr: networkPoolIp.ipAddress ?: "func:nextavailableip:${networkPool.externalId}".toString()]],
						configure_for_dns: false
					]
				} else {
					body = [
						name: shortHostname,
						network_view: networkView,
						ipv4addrs: [[configure_for_dhcp: false, ipv4addr: networkPoolIp.ipAddress ?: "func:nextavailableip:${networkPool.externalId}".toString()]],
						configure_for_dns: false
					]
				}
			} else {
				if(poolServer.serviceMode == 'dhcp' && networkPoolIp.macAddress) {
					body = [
						name: shortHostname,
						network_view: networkView,
						ipv6addrs: [[configure_for_dhcp: true, mac: networkPoolIp.macAddress, ipv6addr: networkPoolIp.ipAddress ?: "func:nextavailableip:${networkPool.externalId}".toString()]],
						configure_for_dns: false
					]
				} else {
					body = [
						name: shortHostname,
						network_view: networkView,
						ipv6addrs: [[configure_for_dhcp: false, ipv6addr: networkPoolIp.ipAddress ?: "func:nextavailableip:${networkPool.externalId}".toString()]],
						configure_for_dns: false
					]
				}
			}
			def extraAttributes
			if (poolServer.configMap?.extraAttributes) {
				extraAttributes = generateExtraAttributes(poolServer, [username: networkPoolIp.createdBy?.username, userId: networkPoolIp.createdBy?.id, dateCreated: MorpheusUtils.formatDate(new Date())])
				body.extattrs = extraAttributes
			}

			log.debug("body: ${body}")
			def results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers: ['Content-Type': 'application/json'], ignoreSSL: poolServer.ignoreSsl,
					body: body), 'POST')
			if (results.success) {
				def ipPath = results.content.substring(1, results.content.length() - 1)
				def ipResults = getItem(client, poolServer, ipPath, [:])
				def newIp
				log.debug("ip results: {}", ipResults)
				if (networkPool.type.code == 'infoblox') {
					newIp = ipResults.results.ipv4addrs?.first()?.ipv4addr
				} else {
					newIp = ipResults.results.ipv6addrs?.first()?.ipv6addr
				}
				networkPoolIp.externalId = ipResults.results?.getAt('_ref')
				networkPoolIp.ipAddress = newIp
				if(createARecord) {
					networkPoolIp.domain = domain
				}
				if (networkPoolIp.id) {
					networkPoolIp = morpheus.network.pool.poolIp.save(networkPoolIp)?.blockingGet()
				} else {
					networkPoolIp = morpheus.network.pool.poolIp.create(networkPoolIp)?.blockingGet()
				}


				if (createARecord && domain) {
					def aRecordRef
					def domainRecord
					def ptrName
					def ptrDomainRecord
					if (networkPool.type.code == 'infoblox') {
						apiPath = getServicePath(poolServer.serviceUrl) + 'record:a'
						body = [
								name    : hostname,
								ipv4addr: newIp
						]
						if (extraAttributes) {
							body.extattrs = extraAttributes
						}
						results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers: ['Content-Type': 'application/json'], ignoreSSL: poolServer.ignoreSsl,
								body: body, contentType: ContentType.APPLICATION_JSON), 'POST')
						if (!results.success) {
							log.warn("A Record Creation Failed")
						} else {

							aRecordRef = results.content.substring(1, results.content.length() - 1)
							domainRecord = new NetworkDomainRecord(networkDomain: domain, networkPoolIp: networkPoolIp, name: hostname, fqdn: hostname, source: 'user', type: 'A', externalId: aRecordRef)
							domainRecord.content = newIp
							morpheus.network.domain.record.create(domainRecord).blockingGet()
							networkPoolIp.internalId = aRecordRef
						}
						if (createPtrRecord) {
							// create PTR Record
							ptrName = "${newIp.tokenize('.').reverse().join('.')}.in-addr.arpa.".toString()
							apiPath = getServicePath(poolServer.serviceUrl) + 'record:ptr'
							body = [
									name    : ptrName,
									ptrdname: hostname,
									ipv4addr: newIp
							]
							if (extraAttributes) {
								body.extattrs = extraAttributes
							}
							results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers: ['Content-Type': 'application/json'], ignoreSSL: poolServer.ignoreSsl,
									body: body), 'POST')
							if (!results.success) {
								log.warn("PTR Record Creation Failed")
							} else {
								String prtRecordRef = results.content.substring(1, results.content.length() - 1)
								ptrDomainRecord = new NetworkDomainRecord(networkDomain: domain, networkPoolIp: networkPoolIp, name: ptrName, fqdn: hostname, source: 'user', type: 'PTR', externalId: prtRecordRef)
								morpheus.network.domain.record.create(ptrDomainRecord).blockingGet()
								log.info("got PTR record: {}", results)
								networkPoolIp.ptrId = prtRecordRef
							}
						}
					} else {
						apiPath = getServicePath(poolServer.serviceUrl) + 'record:aaaa'
						body = [
								name    : hostname,
								ipv6addr: newIp
						]
						if (extraAttributes) {
							body.extattrs = extraAttributes
						}
						results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers: ['Content-Type': 'application/json'], ignoreSSL: poolServer.ignoreSsl,
								body: body, contentType: ContentType.APPLICATION_JSON), 'POST')
						if (!results.success) {
							log.warn("A Record Creation Failed")
						} else {

							aRecordRef = results.content.substring(1, results.content.length() - 1)
							domainRecord = new NetworkDomainRecord(networkDomain: domain, networkPoolIp: networkPoolIp, name: hostname, fqdn: hostname, source: 'user', type: 'AAAA', externalId: aRecordRef)
							domainRecord.content = newIp
							morpheus.network.domain.record.create(domainRecord).blockingGet()
							networkPoolIp.internalId = aRecordRef
						}
						if (createPtrRecord) {
							// create PTR Record
							ptrName = "${newIp.tokenize('.').reverse().join('.')}.in-addr.arpa.".toString()
							apiPath = getServicePath(poolServer.serviceUrl) + 'record:ptr'
							body = [
									name    : ptrName,
									ptrdname: hostname,
									ipv6addr: newIp
							]
							if (extraAttributes) {
								body.extattrs = extraAttributes
							}
							results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers: ['Content-Type': 'application/json'], ignoreSSL: poolServer.ignoreSsl,
									body: body), 'POST')
							if (!results.success) {
								log.warn("PTR Record Creation Failed")
							} else {
								String prtRecordRef = results.content.substring(1, results.content.length() - 1)
								ptrDomainRecord = new NetworkDomainRecord(networkDomain: domain, networkPoolIp: networkPoolIp, name: ptrName, fqdn: hostname, source: 'user', type: 'PTR', externalId: prtRecordRef)
								morpheus.network.domain.record.create(ptrDomainRecord).blockingGet()
								log.info("got PTR record: {}", results)
								networkPoolIp.ptrId = prtRecordRef
							}
						}
					}
					networkPoolIp = morpheus.network.pool.poolIp.save(networkPoolIp)?.blockingGet()
				}
				return ServiceResponse.success(networkPoolIp)
			} else {
				def resultContent = results.content ? new JsonSlurper().parseText(results.content) : [:]
				return ServiceResponse.error("Error allocating host record to the specified ip: ${resultContent?.text}", null, networkPoolIp)
			}
		} finally {
			client.callApi(poolServer.serviceUrl, getServicePath(poolServer.serviceUrl) + 'logout', poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions([headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl]), 'POST')
			client.shutdownClient()
		}
	}

	@Override // FIXME: This method signature is different than infobloxnps
	ServiceResponse updateHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp) {
		HttpApiClient client = new HttpApiClient()
		def serviceUrl = cleanServiceUrl(poolServer.serviceUrl)
		try {

			def apiPath = getServicePath(poolServer.serviceUrl) + networkPoolIp.externalId
			log.debug("url: ${serviceUrl} path: ${apiPath}")
			def hostname = networkPoolIp.hostname

			def body = [
				name:hostname
			]
			log.debug("body: ${body}")
			def results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl,
																												body:body), 'PUT')
			if(results.success) {
				def ipPath = results.content.substring(1, results.content.length() - 1)
				def ipResults = getItem(client, poolServer, ipPath, [:])
				networkPoolIp.externalId = ipResults.results?.getAt('_ref')
				return ServiceResponse.success(networkPoolIp)
			} else {
				return ServiceResponse.error(results.error ?: 'Error Updating Host Record', null, networkPoolIp)
			}
		} finally {
			client.callApi(serviceUrl, getServicePath(serviceUrl) + 'logout', poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions([headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl]), 'POST')
			client.shutdownClient()
		}
	}

//	@Override
	ServiceResponse deleteHostRecord(NetworkPool networkPool, NetworkPoolIp poolIp, Boolean deleteAssociatedRecords) {
		HttpApiClient client = new HttpApiClient();
		def poolServer = morpheus.network.getPoolServerById(networkPool.poolServer.id).blockingGet()
		try {
			if(poolIp.externalId) {
				def serviceUrl = cleanServiceUrl(poolServer.serviceUrl)
				def apiPath = getServicePath(poolServer.serviceUrl) + poolIp.externalId
				def results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl,
																													contentType:ContentType.APPLICATION_JSON), 'DELETE')
				if(results?.success && !results.hasErrors()) {
					if(poolIp.internalId) {
						apiPath = getServicePath(poolServer.serviceUrl) + poolIp.internalId
						//we have an A Record to delete
						results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl), 'DELETE')
					}
					if(poolIp.ptrId) {
						apiPath = getServicePath(poolServer.serviceUrl) + poolIp.ptrId
						//we have an A Record to delete
						results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl,
																														contentType:ContentType.APPLICATION_JSON), 'DELETE')
						log.info("Clearing out PTR Record ${results?.success}")
					}
					return ServiceResponse.success(poolIp)
				} else {
					return ServiceResponse.error(results.error ?: 'Error Deleting Host Record', null, poolIp)
				}
			} else {
				return ServiceResponse.error("Record not associated with corresponding record in target provider", null, poolIp)
			}
		} finally {
			client.callApi(poolServer.serviceUrl, getServicePath(poolServer.serviceUrl) + 'logout', poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions([headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl]), 'POST')
			client.shutdownClient()
		}
	}

	private ServiceResponse getItem(HttpApiClient client, NetworkPoolServer poolServer, String path, Map opts) {
		def rtn = new ServiceResponse(success: false)
		def serviceUrl = cleanServiceUrl(poolServer.serviceUrl)
		def apiPath = getServicePath(poolServer.serviceUrl) + path
		log.debug("url: ${serviceUrl} path: ${apiPath}")
		def results = client.callApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers:['Content-Type':'application/json'], ignoreSSL: poolServer.ignoreSsl,
																											contentType:ContentType.APPLICATION_JSON), 'GET')
		rtn.success = results?.success && !results?.hasErrors()
		log.debug("getItem results: ${results}")
		if(rtn.success) {
			rtn.results = results.content ? new JsonSlurper().parseText(results.content) : [:]
			rtn.headers = results.headers
		}
		return rtn
	}

	private ServiceResponse listNetworks(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts) {
		try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + networksPath         
            def doPaging = opts.doPaging != null ? opts.doPaging : true
            def hasMore = true
            def attempt = 0
            def start = 0
            def count = opts.maxResults != null ? opts.maxResults : 100
            while(hasMore && attempt < 1000) {
                attempt++
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.headers = [Authorization: "Token ${token}".toString()]
                requestOptions.queryParams = [type: allNetwork, start:start.toString(), count:count.toString()]
                if(opts.parentId) {
                    requestOptions.queryParams.parentId = opts.parentId.toString()
                }

                def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                    if(results.data?.size() > 0) {
                        rtn.networks += results.data
                        if(doPaging == true && results.data?.size() >= count) {
                            start += count
                        } else {
                            hasMore = false
                        }
                    } else {
                        //no more content
                        hasMore = false
                    }
                } else {
                    //error
                    hasMore = false
                    //check for bad creds
                    if(results.errorCode == 401i) {
                        rtn.errorCode = 401i
                        rtn.invalidLogin = true
                        rtn.success = true
                        rtn.error = true
                        rtn.msg = results.content ?: 'invalid credentials'
                    } else if(results.errorCode == 400i) {
                        //request
                        rtn.errorCode = 400i
                        //consider this success - just no content
                        rtn.success = true
                        rtn.error = false
                        rtn.msg = results.content ?: 'invalid api request'
                    } else {
                        rtn.errorCode = results.errorCode ?: 500i
                        rtn.success = false
                        rtn.error = true
                        rtn.msg = results.content ?: 'unknown api error'
                        log.warn("error: ${rtn.errorCode} - ${rtn.content}")
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
	void cacheIpAddressRecords(HttpApiClient client, NetworkPoolServer poolServer, Map opts) {
		morpheus.network.pool.listIdentityProjections(poolServer.id).buffer(50).flatMap { Collection<NetworkPoolIdentityProjection> poolIdents ->
			return morpheus.network.pool.listById(poolIdents.collect{it.id})
		}.flatMap { NetworkPool pool ->
			def listResults = listHostRecords(client, poolServer, pool, opts)
			if (listResults.success && listResults.data != null) {
				List<Map> apiItems = listResults.data as List<Map>
				Observable<NetworkPoolIpIdentityProjection> poolIps = morpheus.network.pool.poolIp.listIdentityProjections(pool.id)
				SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp> syncTask = new SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp>(poolIps, apiItems)
				return syncTask.addMatchFunction { NetworkPoolIpIdentityProjection domainObject, Map apiItem ->
					domainObject.externalId == apiItem.'_ref'
				}.addMatchFunction { NetworkPoolIpIdentityProjection domainObject, Map apiItem ->
					domainObject.ipAddress == apiItem.ip_address
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
		}.subscribe()

	}

	void addMissingIps(NetworkPool pool, List addList) {

		List<NetworkPoolIp> poolIpsToAdd = addList?.collect { it ->
			def ipAddress = it.ip_address
			def types = it.types
			def names = it.names
			def ipType = 'assigned'
			if(types?.contains('UNMANAGED')) {
				ipType = 'unmanaged'
			}
			if(!types) {
				ipType = 'used'
			}
			def addConfig = [networkPool: pool, networkPoolRange: pool.ipRanges ? pool.ipRanges.first() : null, ipType: ipType, hostname: names ? names.first() : null, ipAddress: ipAddress, externalId:it.'_ref',]
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
				def hostname = update.masterItem.names ? update.masterItem.names.first() : null
				def ipType = 'assigned'
				if(update.masterItem.types?.contains('UNMANAGED')) {
					ipType = 'unmanaged'
				}
				if(!update.masterItem.types) {
					ipType = 'used'
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
			morpheus.network.pool.poolIp.save(ipsToUpdate)
		}
	}


	ServiceResponse listHostRecords(HttpApiClient client, NetworkPoolServer poolServer, NetworkPool networkPool, Map opts) {
		def rtn = new ServiceResponse()
		rtn.data = []
		def networkView = networkPool.externalId.tokenize('/')[3]
		def serviceUrl = cleanServiceUrl(poolServer.serviceUrl) //ipv4address?network=10.10.10.0/24
		def apiPath
		if (networkPool.type.code == 'infoblox') {
			apiPath = getServicePath(poolServer.serviceUrl) + 'ipv4address'
		} else {
			apiPath = getServicePath(poolServer.serviceUrl) + 'ipv6address'
		}
		log.debug("url: ${serviceUrl} path: ${apiPath}")
		def hasMore = true
		def doPaging = opts.doPaging != null ? opts.doPaging : true
		def maxResults = opts.maxResults ?: 256
		def pageId = null
		def attempt = 0
		def pageQuery = [network: networkPool.name, network_view: networkView, status: 'USED','_return_as_object':'1' ,'_paging':'1', '_max_results':maxResults.toString()]

		while(hasMore && attempt < 1000) {
			if(pageId != null)
				pageQuery['_page_id'] = pageId
			//load results
			def results = client.callJsonApi(serviceUrl, apiPath, poolServer.credentialData?.username ?: poolServer.serviceUsername, poolServer.credentialData?.password ?: poolServer.servicePassword, new HttpApiClient.RequestOptions(headers:['Content-Type':'application/json'],
																												queryParams:pageQuery, ignoreSSL: poolServer.ignoreSsl), 'GET')
			log.debug("listIp4 results: {}",results)
			if(results?.success && !results?.hasErrors()) {
				rtn.success = true

				rtn.headers = results.headers
				def pageResults = results.data

				if(pageResults?.result?.size() > 0) {
					if(pageResults.next_page_id)
						pageId = pageResults.next_page_id
					else
						hasMore = false
					rtn.data += pageResults.result
				} else {
					hasMore = false
				}
			} else {
				if(!rtn.success) {
					rtn.msg = results.error
				}
				hasMore = false
			}
			attempt++
		}
		return rtn
	}

	ServiceResponse testNetworkPoolServer(HttpApiClient client, NetworkPoolServer poolServer) {
		def rtn = new ServiceResponse()
		try {
			def opts = [doPaging:false, maxResults:1]
			def networkList = listNetworks(client, poolServer, opts)
			rtn.success = networkList.success
			rtn.data = [:]
			if(!networkList.success) {
				rtn.msg = 'error connecting to infoblox'
			}
		} catch(e) {
			rtn.success = false
			log.error("test network pool server error: ${e}", e)
		}
		return rtn
	}

	/**
	 * An IPAM Provider can register pool types for display and capability information when syncing IPAM Pools
	 * @return a List of {@link NetworkPoolType} to be loaded into the Morpheus database.
	 */
	Collection<NetworkPoolType> getNetworkPoolTypes() {
		return [
			new NetworkPoolType(code:'netbox', name:'NetBox', creatable:false, description:'NetBox', rangeSupportsCidr: false),
			new NetworkPoolType(code:'netboxipv6', name:'NetBox IPv6', creatable:false, description:'NetBox IPv6', rangeSupportsCidr: true, ipv6Pool:true)
		]
	}

	/**
	 * Provide custom configuration options when creating a new {@link AccountIntegration}
	 * @return a List of OptionType
	 */
	@Override
	List<OptionType> getIntegrationOptionTypes() {
		return [
				new OptionType(code: 'netbox.serviceUrl', name: 'Service URL', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUrl', fieldLabel: 'API Url', fieldContext: 'domain', placeHolder: 'https://x.x.x.x/wapi/v2.2.1', helpBlock: 'Warning! Using HTTP URLS are insecure and not recommended.', displayOrder: 0, required:true),
				new OptionType(code: 'netbox.credentials', name: 'Credentials', inputType: OptionType.InputType.CREDENTIAL, fieldName: 'type', fieldLabel: 'Credentials', fieldContext: 'credential', required: true, displayOrder: 1, defaultValue: 'local',optionSource: 'credentials',config: '{"credentialTypes":["username-password"]}'),

				new OptionType(code: 'netbox.serviceUsername', name: 'Service Username', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUsername', fieldLabel: 'Username', fieldContext: 'domain', displayOrder: 2,localCredential: true, required: true),
				new OptionType(code: 'netbox.servicePassword', name: 'Service Password', inputType: OptionType.InputType.PASSWORD, fieldName: 'servicePassword', fieldLabel: 'Password', fieldContext: 'domain', displayOrder: 3,localCredential: true, required: true),
				new OptionType(code: 'netbox.throttleRate', name: 'Throttle Rate', inputType: OptionType.InputType.NUMBER, defaultValue: 0, fieldName: 'serviceThrottleRate', fieldLabel: 'Throttle Rate', fieldContext: 'domain', displayOrder: 4),
				new OptionType(code: 'netbox.ignoreSsl', name: 'Ignore SSL', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'ignoreSsl', fieldLabel: 'Disable SSL SNI Verification', fieldContext: 'domain', displayOrder: 5),
				new OptionType(code: 'netbox.inventoryExisting', name: 'Inventory Existing', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'inventoryExisting', fieldLabel: 'Inventory Existing', fieldContext: 'config', displayOrder: 6)
		]
	}

	@Override
	Icon getIcon() {
		return new Icon(path:"netbox.svg", darkPath: "netbox.svg")
	}

	/**
	 * Periodically called to refresh and sync data coming from the relevant integration. Most integration providers
	 * provide a method like this that is called periodically (typically 5 - 10 minutes). DNS Sync operates on a 10min
	 * cycle by default. Useful for caching DNS Records created outside of Morpheus.
	 * NOTE: This method is unused when paired with a DNS Provider so simply return null
	 * @param integration The Integration Object contains all the saved information regarding configuration of the DNS Provider.
	 */
	@Override
	void refresh(AccountIntegration integration) {
	 //NOOP
	}

	/**
	 * Validation Method used to validate all inputs applied to the integration of an DNS Provider upon save.
	 * If an input fails validation or authentication information cannot be verified, Error messages should be returned
	 * via a {@link ServiceResponse} object where the key on the error is the field name and the value is the error message.
	 * If the error is a generic authentication error or unknown error, a standard message can also be sent back in the response.
	 * NOTE: This is unused when paired with an IPAMProvider interface
	 * @param integration The Integration Object contains all the saved information regarding configuration of the DNS Provider.
	 * @param opts any custom payload submission options may exist here
	 * @return A response is returned depending on if the inputs are valid or not.
	 */
	@Override
	ServiceResponse verifyAccountIntegration(AccountIntegration integration, Map opts) {
		//NOOP
		return null
	}

    def login(HttpApiClient client, rpcConfig) {
        def rtn = [success:false]
        try {
            HttpApiClient.RequestOptions apiOpts = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSsl)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + authPath
            apiOpts.body = JsonOutput.toJson([username:rpcConfig.username, password:rpcConfig.password])
            def results = client.callJsonApi(apiUrl,apiPath,null,null,apiOpts,"POST")

            if(results?.success && results?.error != true) {
                log.debug("login: ${results}")
                rtn.token = results.key.trim()
                rtn.success = true
            } else {
                //error
            }
        } catch(e) {
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
                password:poolServer.credentialData?.password ?: poolServer.servicePassword,
                serviceUrl:poolServer.serviceUrl,
                ignoreSSL: poolServer.ignoreSsl
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
		if(!rtn?.endsWith('/'))
			rtn = rtn + '/'
		return rtn
	}
}
