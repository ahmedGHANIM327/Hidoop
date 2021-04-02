package config;
/**JobConf represents a MapReduce job configuration.
 *JobConf is the primary interface for a user to describe a MapReduce job to
 *the Hidoop framework for execution.
 * The framework tries to faithfully execute the job as described by JobConf
 */
public interface JobConf {
    /**where the output files should be written 
     */
    public void setOutputPath(String path);
    /**where the input file is
     */
    public void setInputPath(String path);
}