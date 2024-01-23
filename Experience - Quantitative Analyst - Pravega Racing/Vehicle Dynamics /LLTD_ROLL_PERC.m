clc
clear all
fr=3;%rear_frequency
ff=2.5;%front_frequencygf
kt=110;%tire_rate
W=303;%weight_of_car_with_driver
Wr=0.53*W;%rear_weight
Wf=0.47*W;%front_weight
Mr=1;%motion_ratio
rchf=0.025;rchr=0.035;tf=1.25;tr=1.1;
Ay=1.6;rg=0.75;H=0.25;L=1.6;
krf=((2*pi*ff)^2)*Wf/9.81;%front_ride_rate
krr=((2*pi*fr)^2)*Wr/9.81;%rear_ride_rate
kwf=(krf*kt)/(-kt+krf);%front_wheel_rate
kwr=(krr*kt)/(-kt+krr);%rear_wheel_rate
ksf=kwf*Mr%front_spring_rate
ksr=kwr*Mr%rear_spring_rate
fo=Wf/2;fi=Wf/2;ro=Wr/2;ri=Wr/2;
Kdes=(W*H)/rg;
b=(Wf*L)/W;a=(Wr*L)/W;
for perc_roll_f=0:0.01:1
    perc_roll_f
    KF=Kdes*perc_roll_f;
    KR=Kdes*(1-perc_roll_f);
    WTf=Ay*(W/tf)*(((H*KF)/(KF+KR))+(b/L)*rchf);
    WTr=Ay*(W/tr)*(((H*KR)/(KF+KR))+(a/L)*rchr);
    Ffo=fo+WTf;
    Ffi=fi-WTf;
    Fro=ro+WTr;
    Fri=ri-WTr;
end
hold on
plot(perc_roll_f,Ffo)
plot(perc_roll_f,Ffi)
plot(perc_roll_f,Fro)
plot(perc_roll_f,Fri)
hold off