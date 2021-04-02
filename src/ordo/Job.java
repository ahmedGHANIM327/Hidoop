package ordo;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.io.*;
import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import hdfs.HdfsFile;
import hdfs.HdfsServer;
import hdfs.NameProvider;
import map.MapReduce;
import config.ParametersManager;

public class Job implements JobInterfaceX {

    private Format.Type outputFormat;
    private String outputFname;

    private Format.Type inputFormat;
    private String inputFname;

    private int numberOfReduces;
    private int numberOfMaps;

    private SortComparator sortComparator;
    static String NameProviderIPAddress = "localhost";
        
    //available workers and their Server addresses.
	private ArrayList<String> workersURLs;
    
    /**Le comportement de startJob est de lancer des map (avec runMap) 
     *sur tous les démons(worker) des machines, puis attendre que tous les map 
     *soient terminés. Les map ont généré des fichiers locaux
     *sur les machines qui sont des fragments. On peut alors récupérer 
     *le fichier global avec HDFS, puis appliquer le reduce localement.
     */
    public void startJob (MapReduce mr){

    	before();

    	//create a list of workers from the urls list
        List<Worker> workers = getWorkers();

        //dans un premier temps je suppose qu'il y a un map par data-node
        setNumberOfMaps(workers.size());
        long t1 = System.currentTimeMillis();
        try{
        	CallBack cb = new CallBackImpl(workers.size());
        	//launch maps 
            System.out.println("Launching  maps ...");
        	for (int i = 0; i < getNumberOfMaps(); i++) {
                Format format;
                if (getInputFormat() == Format.Type.KV) {
                    format = new KVFormat(this.getInputFname()+".hdfschunk_"+i);
                } else {
                    format = new LineFormat(this.getInputFname()+".hdfschunk_"+i);
                }
        		workers.get(i).runMap(mr, format, 
                new KVFormat(getInputFname()+ ParametersManager.MAP_SUFFIX + ".hdfschunk_" +i), cb);
                
        	}

        	//waiting for maps to be done
            System.out.println("Waiting for maps to be done ...");
        	while (!cb.AllMapsDone()) {
        		//System.out.println("Waiting for maps to be done ...");
        	}
            //Save intermidiary files in the NameProvider
            NameProvider provider;
            try {
                provider = (NameProvider)Naming.lookup("//"+ 
                                    NameProviderIPAddress+":4000/NameProvider");

                HdfsFile file = new HdfsFile(getInputFname()+ ParametersManager.MAP_SUFFIX, 
                                        getChunkDistributionOfMapFiles(provider), 
                                        getNumberOfMaps(), Format.Type.KV);
                provider.addFile(file);

            } catch (Exception e) {

                System.out.println("Couldn't find the NameProvider (from Job)!");

            }

        } catch (RemoteException e) {

            System.out.println("Couldn't connect to the workers !");
            e.printStackTrace();
            System.exit(0);
        }

        // Retrieving map results
		try {

            System.out.println("Retrieving map results..");
            System.out.println(getInputFname()+ ParametersManager.MAP_SUFFIX);
			HdfsClient.HdfsRead(getInputFname()+ ParametersManager.MAP_SUFFIX , getInputFname() +"-temp");

		} catch (Exception e) {

            System.out.println("Problem encountered during the Retrieving of map results");
			e.printStackTrace();
            System.exit(0);
		} 

        // joigning the map chunks in the same file ***-temp
    	Format joinMaps = new KVFormat(getInputFname() +"-temp");
		Format finalRes = new KVFormat(getOutputFname());
		joinMaps.open(Format.OpenMode.R);
		finalRes.open(Format.OpenMode.W);
		// Starting reduce
        System.out.println("Starting reduce...");
		mr.reduce(joinMaps, finalRes);
		joinMaps.close();
        File fileTemp = new File(getInputFname() +"-temp");
        //delete the temporary file created before reduce
        fileTemp.delete();
		finalRes.close();
        long t2 = System.currentTimeMillis();
        System.out.println("execution time of StartJob in ms ="+(t2-t1));
    }

    /**
     * get the list of workers from their urls saved in config/conf.txt file
     * @return list of workers
     */
    private List<Worker> getWorkers(){
    	List<Worker> workers = new ArrayList<Worker>();
        for(String url : workersURLs) {
			try {
				workers.add((Worker)Naming.lookup(url));
			} catch (Exception e) {
				System.out.println("the worker with url: " + url + " not found.");
			}
        }
        return workers;
    }


    /**prepare the environment before starting
    */
    private void before() {
    	setOutputFname(getInputFname() + ParametersManager.RESULT_SUFFIX);
        workersURLs=getWorkersUrls();
    }


    /** Load servers Urls from file conf.txt
    */
    private ArrayList<String>  getWorkersUrls()  {
        ArrayList<String> l = new ArrayList<String>();
        try {
            File file = new File("config/conf.txt");    
            // Créer l'objet File Reader
            FileReader fr = new FileReader(file);  
            // Créer l'objet BufferedReader        
            BufferedReader br = new BufferedReader(fr);
            String line;
            while((line = br.readLine()) != null) {
              // ajoute la ligne à la liste
                l.add(line);   
            }
            fr.close();  
            
        } catch(IOException e) {
            System.out.println("Couldn't load configuration file, please try again!");
        }
        return l;
    }

    /*Get the chuns distribution of a file*/
    public HashMap<String, HdfsServer> getChunkDistributionOfMapFiles(NameProvider np) 
                                                        throws RemoteException{

        HashMap<String, HdfsServer> distribution = new HashMap<String, HdfsServer>();
        List<HdfsServer> l = np.getServers();
        for (int i=0;i<l.size();i++) {
            distribution.put(getInputFname()+ ParametersManager.MAP_SUFFIX + 
                                                    ".hdfschunk_"+ i, l.get(i));
        }
        return distribution;
    }

    //Setters
    public void setNumberOfReduces(int tasks){
        this.numberOfReduces=tasks;
    }


    public void setNumberOfMaps(int tasks){
        this.numberOfMaps= tasks;
    }


    public void setOutputFormat(Format.Type ft){
        this.outputFormat = ft;
    }


    public void setOutputFname(String fname){
        this.outputFname = fname;
    }


    public void setInputFormat(Format.Type ft){
        this.inputFormat = ft;
    }


    public void setInputFname(String fname){
        this.inputFname = fname;
    }


    public void setSortComparator(SortComparator sc){
        this.sortComparator = sc;
    }
    //Getters

    public int getNumberOfReduces(){
        return this.numberOfReduces;
    }


    public int getNumberOfMaps(){
        return this.numberOfMaps;
    }


    public Format.Type getInputFormat(){
        return this.inputFormat;
    }


    public Format.Type getOutputFormat(){
        return this.outputFormat;
    }


    public String getInputFname(){
        return this.inputFname;
    }


    public String getOutputFname(){
        return this.outputFname;
    }


    public SortComparator getSortComparator(){
        return this.sortComparator;
    }

}
