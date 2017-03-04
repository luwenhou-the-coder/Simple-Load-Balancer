
import com.microsoft.azure.management.compute.ComputeManagementClient;
import com.microsoft.azure.management.compute.ComputeManagementService;
import com.microsoft.azure.management.compute.models.CachingTypes;
import com.microsoft.azure.management.compute.models.DiskCreateOptionTypes;
import com.microsoft.azure.management.compute.models.HardwareProfile;
import com.microsoft.azure.management.compute.models.NetworkInterfaceReference;
import com.microsoft.azure.management.compute.models.NetworkProfile;
import com.microsoft.azure.management.compute.models.OSDisk;
import com.microsoft.azure.management.compute.models.OSProfile;
import com.microsoft.azure.management.compute.models.OperatingSystemTypes;
import com.microsoft.azure.management.compute.models.StorageProfile;
import com.microsoft.azure.management.compute.models.VirtualHardDisk;
import com.microsoft.azure.management.compute.models.VirtualMachine;
import com.microsoft.azure.management.network.NetworkResourceProviderClient;
import com.microsoft.azure.management.network.NetworkResourceProviderService;
import com.microsoft.azure.management.network.models.AzureAsyncOperationResponse;
import com.microsoft.azure.management.network.models.DhcpOptions;
import com.microsoft.azure.management.network.models.PublicIpAddressGetResponse;
import com.microsoft.azure.management.network.models.VirtualNetwork;
import com.microsoft.azure.management.resources.ResourceManagementClient;
import com.microsoft.azure.management.resources.ResourceManagementService;
import com.microsoft.azure.management.storage.StorageManagementClient;
import com.microsoft.azure.management.storage.StorageManagementService;
import com.microsoft.azure.utility.AuthHelper;
import com.microsoft.azure.utility.ComputeHelper;
import com.microsoft.azure.utility.NetworkHelper;
import com.microsoft.azure.utility.ResourceContext;
import com.microsoft.azure.utility.StorageHelper;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;

import java.io.IOException;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;

import java.util.Set;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoadBalancer {

    private static final int THREAD_POOL_SIZE = 4;
    private final ServerSocket socket;
    private final DataCenterInstance[] instances;
    private int sendTo = 0;
    private Set unhealthy = new HashSet();

    private static ResourceManagementClient resourceManagementClient;
    private static StorageManagementClient storageManagementClient;
    private static ComputeManagementClient computeManagementClient;
    private static NetworkResourceProviderClient networkResourceProviderClient;

    private static String sourceVhdUri_DC = "";
    // configuration for your application token
    private static final String baseURI = "https://management.azure.com/";
    private static final String basicURI = "https://management.core.windows.net/";
    private static final String endpointURL = "https://login.windows.net/";

    private static final String subscriptionId = System.getenv("SUB_ID");  //read from environmental variables
    private static final String tenantID = System.getenv("TEN_ID");     //read from environmental variables
    private static String applicationID = System.getenv("APP_ID");       //read from environmental variables
    private static String applicationKey = System.getenv("APP_KEY");     //read from environmental variables

    // configuration for your resource account/storage account
    private static final String storageAccountName = System.getenv("A_NAME");    //read from environmental variables
    private static final String resourceGroupNameWithVhd = System.getenv("A_VHD_NAME");  //read from environmental variables

    private static final String size_DC = "Standard_A1";
    private static final String region = "EastUs";
    private static String vmName_DC = "";
    private static String resourceGroupName = "";
    private static String dns_DC = "";
    private static String dns_DC_back = "";

    // configuration for your virtual machine
    private static final String adminName = "ubuntu";
    /**
     * Password requirements: 1) Contains an uppercase character 2) Contains a
     * lowercase character 3) Contains a numeric digit 4) Contains a special
     * character.
     */
    private static final String adminPassword = "Cloud@123";

    public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) throws Exception {
        this.socket = socket;
        this.instances = instances;
        Configuration config = createConfiguration();
        resourceManagementClient = ResourceManagementService.create(config);
        storageManagementClient = StorageManagementService.create(config);
        computeManagementClient = ComputeManagementService.create(config);
        networkResourceProviderClient = NetworkResourceProviderService.create(config);
        sourceVhdUri_DC = String.format("https://%s.blob.core.windows.net/system/Microsoft.Compute/Images/vhds/%s",
                storageAccountName, "cc15619p22dcv6-osDisk.b0c453f3-f75f-4a2d-bd9c-ae055b830124.vhd");

    }

    public static Configuration createConfiguration() throws Exception {
        // get token for authentication
        String token = AuthHelper.getAccessTokenFromServicePrincipalCredentials(
                basicURI,
                endpointURL,
                tenantID,
                applicationID,
                applicationKey).getAccessToken();

        // generate Azure sdk configuration manager
        return ManagementConfiguration.configure(
                null, // profile
                new URI(baseURI), // baseURI
                subscriptionId, // subscriptionId
                token// token
        );
    }

    /**
     * *
     * Create a virtual machine given configurations.
     *
     * @param resourceGroupName: a new name for your virtual machine
     * [customized], will create a new one if not already exist
     * @param vmName: a PUBLIC UNIQUE name for virtual machine
     * @param resourceGroupNameWithVhd: the resource group where the storage
     * account for VHD is copied
     * @param sourceVhdUri: the Uri for VHD you copied
     * @param instanceSize
     * @param subscriptionId: your Azure account subscription Id
     * @param storageAccountName: the storage account where you VHD exist
     * @return created virtual machine IP
     * @throws java.lang.Exception
     */
    public static ResourceContext createVM(
            String resourceGroupName,
            String vmName,
            String resourceGroupNameWithVhd,
            String sourceVhdUri,
            String instanceSize,
            String subscriptionId,
            String storageAccountName) throws Exception {

        ResourceContext contextVhd = new ResourceContext(
                region, resourceGroupNameWithVhd, subscriptionId, false);
        ResourceContext context = new ResourceContext(
                region, resourceGroupName, subscriptionId, false);

        ComputeHelper.createOrUpdateResourceGroup(resourceManagementClient, context);
        context.setStorageAccountName(storageAccountName);
        contextVhd.setStorageAccountName(storageAccountName);
        context.setStorageAccount(StorageHelper.getStorageAccount(storageManagementClient, contextVhd));

        if (context.getNetworkInterface() == null) {
            if (context.getPublicIpAddress() == null) {
                NetworkHelper
                        .createPublicIpAddress(networkResourceProviderClient, context);
            }
            if (context.getVirtualNetwork() == null) {
                NetworkHelper
                        .createVirtualNetwork(networkResourceProviderClient, context);
            }

            VirtualNetwork vnet = context.getVirtualNetwork();

            // set DhcpOptions
            DhcpOptions dop = new DhcpOptions();
            ArrayList<String> dnsServers = new ArrayList<>(2);
            dnsServers.add("8.8.8.8");
            dop.setDnsServers(dnsServers);
            vnet.setDhcpOptions(dop);

            AzureAsyncOperationResponse response = networkResourceProviderClient.getVirtualNetworksOperations()
                    .createOrUpdate(context.getResourceGroupName(), context.getVirtualNetworkName(), vnet);

            NetworkHelper
                    .createNIC(networkResourceProviderClient, context, context.getVirtualNetwork().getSubnets().get(0));

            NetworkHelper
                    .updatePublicIpAddressDomainName(networkResourceProviderClient, resourceGroupName, context.getPublicIpName(), vmName);
        }

        System.out.println("[15319/15619] " + context.getPublicIpName());
        System.out.println("[15319/15619] Start Create VM...");

        try {
            // name for your VirtualHardDisk
            String osVhdUri = ComputeHelper.getVhdContainerUrl(context) + String.format("/os%s.vhd", vmName);

            VirtualMachine vm = new VirtualMachine(context.getLocation());

            vm.setName(vmName);
            vm.setType("Microsoft.Compute/virtualMachines");
            vm.setHardwareProfile(createHardwareProfile(context, instanceSize));
            vm.setStorageProfile(createStorageProfile(osVhdUri, sourceVhdUri));
            vm.setNetworkProfile(createNetworkProfile(context));
            vm.setOSProfile(createOSProfile(adminName, adminPassword, vmName));

            context.setVMInput(vm);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        // Remove the resource group will remove all assets (VM/VirtualNetwork/Storage Account etc.)
        // Comment the following line to keep the VM.
        // resourceManagementClient.getResourceGroupsOperations().beginDeleting(context.getResourceGroupName());
        // computeManagementClient.getVirtualMachinesOperations().beginDeleting(resourceGroupName,"project2.2");
        return context;
    }

    /**
     * *
     * Check public IP address of virtual machine
     *
     * @param context
     * @param vmName
     * @return public IP
     */
    public static String checkVM(ResourceContext context, String vmName) {
        String dns = null;

        try {
            VirtualMachine vmHelper = ComputeHelper.createVM(
                    resourceManagementClient, computeManagementClient, networkResourceProviderClient, storageManagementClient,
                    context, vmName, "ubuntu", "Cloud@123").getVirtualMachine();

            System.out.println("[15319/15619] " + vmHelper.getName() + " Is Created :)");
            while (dns == null) {
                PublicIpAddressGetResponse result = networkResourceProviderClient.getPublicIpAddressesOperations().get(resourceGroupName, context.getPublicIpName());
                dns = result.getPublicIpAddress().getDnsSettings().getDomainNameLabel();
                Thread.sleep(10);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return dns;
    }

    /**
     * *
     * Create a HardwareProfile for virtual machine
     *
     * @param context
     * @param instanceSize
     * @return created HardwareProfile
     */
    public static HardwareProfile createHardwareProfile(ResourceContext context, String instanceSize) {
        HardwareProfile hardwareProfile = new HardwareProfile();
        if (context.getVirtualMachineSizeType() != null && !context.getVirtualMachineSizeType().isEmpty()) {
            hardwareProfile.setVirtualMachineSize(context.getVirtualMachineSizeType());
        } else {
            hardwareProfile.setVirtualMachineSize(instanceSize);
        }
        return hardwareProfile;
    }

    /**
     * *
     * Create a StorageProfile for virtual machine
     *
     * @param osVhdUri
     * @param sourceVhdUri
     * @return created StorageProfile
     */
    public static StorageProfile createStorageProfile(String osVhdUri, String sourceVhdUri) {
        StorageProfile storageProfile = new StorageProfile();

        VirtualHardDisk vHardDisk = new VirtualHardDisk();
        vHardDisk.setUri(osVhdUri);
        //set source image
        VirtualHardDisk sourceDisk = new VirtualHardDisk();
        sourceDisk.setUri(sourceVhdUri);

        OSDisk osDisk = new OSDisk("osdisk", vHardDisk, DiskCreateOptionTypes.FROMIMAGE);
        osDisk.setSourceImage(sourceDisk);
        osDisk.setOperatingSystemType(OperatingSystemTypes.LINUX);
        osDisk.setCaching(CachingTypes.NONE);

        storageProfile.setOSDisk(osDisk);

        return storageProfile;
    }

    /**
     * *
     * Create a NetworkProfile for virtual machine
     *
     * @param context
     * @return created NetworkProfile
     */
    public static NetworkProfile createNetworkProfile(ResourceContext context) {
        NetworkProfile networkProfile = new NetworkProfile();
        NetworkInterfaceReference nir = new NetworkInterfaceReference();
        nir.setReferenceUri(context.getNetworkInterface().getId());
        ArrayList<NetworkInterfaceReference> nirs = new ArrayList<>(1);
        nirs.add(nir);
        networkProfile.setNetworkInterfaces(nirs);

        return networkProfile;
    }

    /**
     * *
     * Create a OSProfile for virtual machine
     *
     * @param adminName
     * @param adminPassword
     * @param vmName
     * @return created OSProfile
     */
    public static OSProfile createOSProfile(String adminName, String adminPassword, String vmName) {
        OSProfile osProfile = new OSProfile();
        osProfile.setAdminPassword(adminPassword);
        osProfile.setAdminUsername(adminName);
        osProfile.setComputerName(vmName);

        return osProfile;
    }

    public static void createDC() throws Exception {
        //create a new DC
        String seed = String.format("%d%d", (int) System.currentTimeMillis() % 1000, (int) (Math.random() * 1000));
        vmName_DC = String.format("dc%s%s", seed, "vm");
        resourceGroupName = String.format("dc%s%s", seed, "ResourceGroup");
        ResourceContext context_DC = createVM(
                resourceGroupName,
                vmName_DC,
                resourceGroupNameWithVhd,
                sourceVhdUri_DC,
                size_DC,
                subscriptionId,
                storageAccountName);
        dns_DC = checkVM(context_DC, vmName_DC);
        System.out.println(dns_DC);
        Thread.sleep(5000);
    }

    public static void createDC_Backup() throws Exception {
        //create a new DC
        String seed = String.format("%d%d", (int) System.currentTimeMillis() % 1000, (int) (Math.random() * 1000));
        vmName_DC = String.format("dc%s%s", seed, "vm");
        resourceGroupName = String.format("dc%s%s", seed, "ResourceGroup");
        ResourceContext context_DC = createVM(
                resourceGroupName,
                vmName_DC,
                resourceGroupNameWithVhd,
                sourceVhdUri_DC,
                size_DC,
                subscriptionId,
                storageAccountName);
        dns_DC_back = checkVM(context_DC, vmName_DC);
        System.out.println(dns_DC_back);
    }

    public static int healthCheck(String url) throws InterruptedException, MalformedURLException, IOException {
        //check the health status of an instance
        URL getUrl = new URL(url);
        HttpURLConnection con = (HttpURLConnection) getUrl.openConnection();
        return con.getResponseCode();
    }

    // Complete this function
    public void start() throws IOException, Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        boolean firstTime = true;
        while (true) {
            try {
                for (int j = 0; j < 4; j++) {   //first give 4 round robins
                    for (sendTo = 0; sendTo < 3; sendTo++) {
                        if (!unhealthy.contains(sendTo)) {
                            Runnable requestHandler = new RequestHandler(socket.accept(), instances[sendTo]);
                            executorService.execute(requestHandler);
                        }
                    }
                }

                for (sendTo = 0; sendTo < 3; sendTo++) {    //check instance health
                    String healthUrl = instances[sendTo].getUrl();
                    int healthResult = healthCheck(healthUrl);
                    if (healthResult != 200) {
                        if (!unhealthy.contains(sendTo)) {  //if the failure is a unrecorded instance
                            createDC();
                            instances[sendTo] = new DataCenterInstance("", "http://" + dns_DC + ".eastus.cloudapp.azure.com");
                            unhealthy.add(sendTo);
                        }
                        System.out.println("response code is not 200");
                    } else {    //the instance is healthy
                        if (unhealthy.contains(sendTo)) {   //if the failure is the recorded instance
                            unhealthy.remove(sendTo);   //remove it from unhealthy set
                            System.out.println("instance " + sendTo + " starts to serve...");
                            System.out.println("check, broken instance: " + unhealthy.toString());
                        }
                        System.out.println("everything OK");
                    }

                }

                for (int j = 0; j < 4; j++) {   //another 4 round robin
                    for (sendTo = 0; sendTo < 3; sendTo++) {
                        if (!unhealthy.contains(sendTo)) {
                            Runnable requestHandler = new RequestHandler(socket.accept(), instances[sendTo]);
                            executorService.execute(requestHandler);
                        }
                    }
                }
                if (sendTo == 3) {
                    sendTo = 0;
                }
            } catch (Exception e) {
                System.out.println("catch send to " + sendTo + " , now broken instance: " + unhealthy.toString());
                if (!unhealthy.contains(sendTo)) {  //if the failure is a unrecorded instance
                    unhealthy.add(sendTo);  //add it to unhealthy set
                    System.out.println("Created by catch");
                    if (firstTime) {    //if it is the first time of failure, create a DC and a DC for backup in case another failure occurs
                        createDC();
                        instances[sendTo] = new DataCenterInstance("", "http://" + dns_DC + ".eastus.cloudapp.azure.com");
                        createDC_Backup();
                        firstTime = false;
                    } else {    //if it is not the first time of failure, use the backup DC and create a new one
                        instances[sendTo] = new DataCenterInstance("", "http://" + dns_DC_back + ".eastus.cloudapp.azure.com");
                        createDC_Backup();
                    }
                    System.out.println("newly added instance" + sendTo + " : " + instances[sendTo].getUrl());
                    System.out.println("now broken instance: " + unhealthy.toString());
                } else {
                    Thread.sleep(5000); //if the failure is the recorded instance, simply skip and wait until new instance is ready
                }
            }
        }

    }
}
