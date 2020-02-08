class StockDay { 
  String company;
  String date;
  double op; 
  double hp; 
  double lp; 
  double cp; 
  long numOfShares; 
  double acp;

  public StockDay(String[] values){
    this.company=values[0];
    this.date=values[1];
    this.op=Double.parseDouble(values[2].trim());
    this.hp=Double.parseDouble(values[3].trim());
    this.lp=Double.parseDouble(values[4].trim());
    this.cp=Double.parseDouble(values[5].trim());
    this.numOfShares=Long.parseLong(values[6].trim());
    this.acp=Double.parseDouble(values[7].trim());
  }
  public double fluctuation(){
    return (hp != 0)?((hp-lp)/hp)*100:0;
  }
  public boolean isCrazy(){
    return fluctuation() >= 15;
  }
  
}
